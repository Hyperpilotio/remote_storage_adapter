package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/gobwas/glob"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/spf13/viper"

	influx "github.com/influxdata/influxdb/client/v2"
	log "github.com/sirupsen/logrus"

	"github.com/hyperpilotio/remote_storage_adapter/pkg/clients/influxdb"
	"github.com/hyperpilotio/remote_storage_adapter/pkg/common"
	hpmodel "github.com/hyperpilotio/remote_storage_adapter/pkg/common/model"
	"github.com/hyperpilotio/remote_storage_adapter/pkg/db"
)

type GlobInfluxdbConfig struct {
	RemoteTimeout        time.Duration
	FilterMetricPatterns []glob.Glob
}

// Server store the stats / data of every customerProfile
type Server struct {
	Config             *viper.Viper
	AuthDB             *db.AuthDB
	GlobInfluxdbConfig *GlobInfluxdbConfig

	customerProfileLock sync.RWMutex
	CustomerProfiles    map[string]*CustomerProfile

	writerLock sync.Mutex
	Writers    map[string]*HyperpilotWriter
}

func init() {
	log.SetLevel(common.GetLevel(os.Getenv("ADAPTER_LOG_LEVEL")))
}

// NewServer return an instance of Server struct.
func NewServer(cfg *viper.Viper) *Server {
	return &Server{
		Config: cfg,
		AuthDB: db.NewAuthDB(cfg),

		GlobInfluxdbConfig: &GlobInfluxdbConfig{
			FilterMetricPatterns: make([]glob.Glob, 0),
		},
		CustomerProfiles: make(map[string]*CustomerProfile),
		Writers:          make(map[string]*HyperpilotWriter),
	}
}

func (server *Server) Init() error {
	remoteTimeout, err := time.ParseDuration(server.Config.GetString("remoteTimeout"))
	if err != nil {
		return errors.New("Unable to parse remoteTimeout duration: %s" + err.Error())
	}
	server.GlobInfluxdbConfig.RemoteTimeout = remoteTimeout

	filterMetricsConfigUrl := server.Config.GetString("filterMetricsConfigUrl")
	if filterMetricsConfigUrl != "" {
		metricsConfig, err := downloadConfigFile(filterMetricsConfigUrl)
		if err != nil {
			return errors.New("Unable to download config file: %s" + err.Error())
		}

		for metricName, _ := range metricsConfig.Metrics {
			pattern, err := glob.Compile(metricName)
			if err != nil {
				return fmt.Errorf("Unable to compile filter metric namespace for %s: %s",
					metricName, err.Error())
			}
			server.GlobInfluxdbConfig.FilterMetricPatterns =
				append(server.GlobInfluxdbConfig.FilterMetricPatterns, pattern)
		}
	}

	customers, err := server.AuthDB.GetCustomers()
	if err != nil {
		return errors.New("Unable to get customers config: %s" + err.Error())
	}
	log.Infof("Shared Mongo has %d customer cluster, create inlfux client for each client cluster", len(customers))
	for _, customerCfg := range customers {
		customerProfile := &CustomerProfile{
			Config: &customerCfg,
		}
		if err := buildClients(&customerCfg, server.GlobInfluxdbConfig, customerProfile); err != nil {
			return fmt.Errorf("Unable to build customer clients %s: %s", customerCfg.CustomerId, err.Error())
		}

		if err := server.CreateHyperpilotWriter(customerProfile); err != nil {
			return fmt.Errorf("Unable tp create hyperpilot writer %s: %s", customerCfg.CustomerId, err.Error())
		}
		server.CustomerProfiles[customerCfg.CustomerId] = customerProfile
	}

	return nil
}

// StartServer start a web servers
func (server *Server) StartServer() error {
	http.Handle(server.Config.GetString("telemetryPath"), prometheus.Handler())
	http.HandleFunc("/write", server.write)
	http.HandleFunc("/read", server.read)
	return http.ListenAndServe(":"+server.Config.GetString("listenAddr"), nil)
}

func (server *Server) CreateHyperpilotWriter(cp *CustomerProfile) error {
	server.writerLock.Lock()
	defer server.writerLock.Unlock()

	writer, ok := server.Writers[cp.Config.CustomerId]
	if ok {
		log.Warnf("Writer customerId {%s} is duplicated, skip this writer", cp.Config.CustomerId)
		cp.HyperpilotWriter = writer
		return nil
	}

	hpWriter, err := NewHyperpilotWriter(server, cp.writer, cp.Config.CustomerId)
	if err != nil {
		return fmt.Errorf("Unable to new writer for customerId={%s}: %s", cp.Config.CustomerId, err.Error())
	}
	hpWriter.Run()

	cp.HyperpilotWriter = hpWriter
	server.Writers[cp.Config.CustomerId] = hpWriter

	return nil
}

type writer interface {
	Write(samples model.Samples) error
	Name() string
}

type reader interface {
	Read(req *prompb.ReadRequest) (*prompb.ReadResponse, error)
	Name() string
}

type CustomerProfile struct {
	Config           *hpmodel.CustomerConfig
	writer           writer
	reader           reader
	HyperpilotWriter *HyperpilotWriter
}

func (server *Server) getCustomerProfile(token, clusterId, customerId string) (*CustomerProfile, error) {
	server.customerProfileLock.RLock()
	defer server.customerProfileLock.RUnlock()

	customerProfile, ok := server.CustomerProfiles[customerId]
	if ok {
		return customerProfile, nil
	}

	customerProfile = &CustomerProfile{
		Config: &hpmodel.CustomerConfig{
			Token:      token,
			CustomerId: customerId,
			ClusterId:  clusterId,
		},
	}

	customerConfig, err := server.AuthDB.GetOneCustomerByToken(token)
	if err != nil {
		log.Error(err.Error())
		return nil, err
	}

	if err := buildClients(customerConfig, server.GlobInfluxdbConfig, customerProfile); err != nil {
		return nil, fmt.Errorf("Unable to build customer clients %s: %s", customerId, err.Error())
	}

	if err := server.CreateHyperpilotWriter(customerProfile); err != nil {
		return nil, fmt.Errorf("Unable tp create hyperpilot writer %s: %s", customerId, err.Error())
	}
	server.CustomerProfiles[customerId] = customerProfile
	return customerProfile, nil
}

func (server *Server) write(w http.ResponseWriter, r *http.Request) {
	token := r.FormValue("token")
	clutserId := r.FormValue("clusterId")
	customerId := r.FormValue("customerId")
	customerProfile, err := server.getCustomerProfile(token, clutserId, customerId)
	if err != nil {
		log.Errorf("CustomerProfile not found: %s", err.Error())
		http.Error(w, "CustomerProfile not found", http.StatusInternalServerError)
		return
	}

	compressed, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Errorf("Read error: %s", err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		log.Errorf("Decode error: %s", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var req prompb.WriteRequest
	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		log.Errorf("Unmarshal error: %s", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	samples := protoToSamples(&req, server.GlobInfluxdbConfig.FilterMetricPatterns)
	receivedSamples.Add(float64(len(samples)))
	customerProfile.HyperpilotWriter.Put(samples)
}

func (server *Server) read(w http.ResponseWriter, r *http.Request) {
	token := r.FormValue("token")
	clutserId := r.FormValue("clusterId")
	customerId := r.FormValue("customerId")
	customerProfile, err := server.getCustomerProfile(token, clutserId, customerId)
	if err != nil {
		log.Errorf("CustomerProfile not found: %s", err.Error())
		http.Error(w, "CustomerProfile not found", http.StatusInternalServerError)
		return
	}

	compressed, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Errorf("Read error: %s", err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		log.Errorf("Decode error: %s", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var req prompb.ReadRequest
	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		log.Errorf("Unmarshal error: %s", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var resp *prompb.ReadResponse
	reader := customerProfile.reader
	resp, err = reader.Read(&req)
	if err != nil {
		log.WithFields(log.Fields{
			"query":   req,
			"storage": reader.Name(),
		}).Warnf("Error executing query: %s", err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	data, err := proto.Marshal(resp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/x-protobuf")
	w.Header().Set("Content-Encoding", "snappy")

	compressed = snappy.Encode(nil, data)
	if _, err := w.Write(compressed); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

type metricInfo struct {
	Version_ int `json:"version":"version"`
}

type MetricsConfig struct {
	Metrics map[string]metricInfo `json:"metrics"`
}

func downloadConfigFile(url string) (*MetricsConfig, error) {
	response, err := http.Get(url)
	if err != nil {
		return nil, errors.New("Unable to download config file: " + err.Error())
	}
	defer response.Body.Close()

	decoder := json.NewDecoder(response.Body)
	configs := MetricsConfig{}
	if err := decoder.Decode(&configs); err != nil {
		return nil, errors.New("Unable to decode body: " + err.Error())
	}

	return &configs, nil
}

func buildClients(customerConfig *hpmodel.CustomerConfig, globalInfluxConfig *GlobInfluxdbConfig, cp *CustomerProfile) error {
	influxdbURL := customerConfig.InfluxdbURL
	if influxdbURL != "" {
		log.Infof("create influx for Customer ID {%s} with influx url {%s}", customerConfig.CustomerId, customerConfig.InfluxdbURL)
		url, err := url.Parse(influxdbURL)
		if err != nil {
			return fmt.Errorf("Failed to parse InfluxDB URL %s: %s", influxdbURL, err.Error())
		}
		conf := influx.HTTPConfig{
			Addr:     url.String(),
			Username: customerConfig.InfluxdbUsername,
			Password: customerConfig.InfluxdbPassword,
			Timeout:  globalInfluxConfig.RemoteTimeout,
		}
		c := influxdb.NewClient(conf, customerConfig.InfluxdbDatabase, customerConfig.InfluxdbRetentionPolicy)
		prometheus.MustRegister(c)
		cp.writer = c
		cp.reader = c
	}

	return nil
}

func protoToSamples(req *prompb.WriteRequest, filterMetricPatterns []glob.Glob) model.Samples {
	var samples model.Samples
	for _, ts := range req.Timeseries {
		metric := make(model.Metric, len(ts.Labels))
		for _, l := range ts.Labels {
			metric[model.LabelName(l.Name)] = model.LabelValue(l.Value)
		}

		metricName := metric[model.MetricNameLabel]
		for _, s := range ts.Samples {
			isAppendMetric := false
			if len(filterMetricPatterns) == 0 {
				isAppendMetric = true
			}

			for _, pattern := range filterMetricPatterns {
				if pattern.Match(fmt.Sprintf("/%s", metricName)) {
					isAppendMetric = true
					break
				}
			}

			if isAppendMetric {
				s := &model.Sample{
					Metric:    metric,
					Value:     model.SampleValue(s.Value),
					Timestamp: model.Time(s.Timestamp),
				}

				v := float64(s.Value)
				if math.IsNaN(v) || math.IsInf(v, 0) {
					s.Value = 0
				}
				samples = append(samples, s)
			}
		}
	}
	return samples
}
