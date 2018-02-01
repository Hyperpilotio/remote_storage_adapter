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

// Server store the stats / data of every customerProfile
type Server struct {
	Config *viper.Viper
	AuthDB *db.AuthDB

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
		Config:           cfg,
		AuthDB:           db.NewAuthDB(cfg),
		CustomerProfiles: make(map[string]*CustomerProfile),
	}
}

func (server *Server) Init() error {
	remoteTimeout, err := time.ParseDuration(server.Config.GetString("remoteTimeout"))
	if err != nil {
		return errors.New("Unable to parse remoteTimeout duration: %s" + err.Error())
	}

	customers, err := server.AuthDB.GetCustomers()
	if err != nil {
		return errors.New("Unable to get customers config: %s" + err.Error())
	}

	filterMetricPatterns := []glob.Glob{}
	filterMetricsConfigUrl := server.Config.GetString("filterMetricsConfigUrl")
	if filterMetricsConfigUrl != "" {
		metricsConfig, err := downloadConfigFile(filterMetricsConfigUrl)
		if err != nil {
			return errors.New("Unable to download config file: %s" + err.Error())
		}

		for metricName, _ := range metricsConfig.Metrics {
			pattern, err := glob.Compile(metricName)
			if err != nil {
				return fmt.Errorf("Unable to compile filter metric namespace for %s: %s", metricName, err.Error())
			}
			filterMetricPatterns = append(filterMetricPatterns, pattern)
		}
	}

	for _, customerCfg := range customers {
		customerProfile := &CustomerProfile{
			Config:               &customerCfg,
			filterMetricPatterns: filterMetricPatterns,
		}

		if err := buildClients(server, customerProfile, remoteTimeout); err != nil {
			return fmt.Errorf("Unable build customer clients %s: %s", customerCfg.CustomerId, err.Error())
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

func (server *Server) CreateWriter(cp *CustomerProfile) error {
	server.writerLock.Lock()
	defer server.writerLock.Unlock()

	hpWriter, err := NewHyperpilotWriter(server, cp.writer, cp.Config.CustomerId)
	if err != nil {
		return fmt.Errorf("Unable to new writer for customerId={%s}: %s", cp.Config.CustomerId, err.Error())
	}
	cp.HyperpilotWriter = hpWriter
	hpWriter.Run()

	if _, ok := server.Writers[cp.Config.CustomerId]; ok {
		log.Warnf("Writer customerId {%s} is duplicated, skip this writer", cp.Config.CustomerId)
		return nil
	}
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
	Config               *hpmodel.CustomerConfig
	writer               writer
	reader               reader
	filterMetricPatterns []glob.Glob
	HyperpilotWriter     *HyperpilotWriter
}

func (server *Server) getCustomerProfile(tokenId string) (*CustomerProfile, error) {
	server.customerProfileLock.RLock()
	defer server.customerProfileLock.RUnlock()

	customerProfile, ok := server.CustomerProfiles[tokenId]
	if !ok {
		return nil, errors.New("Unable to find CustomerProfile")
	}
	return customerProfile, nil
}

func (server *Server) write(w http.ResponseWriter, r *http.Request) {
	tokenId := r.FormValue("tokenId")
	customerProfile, err := server.getCustomerProfile(tokenId)
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

	samples := protoToSamples(&req, customerProfile.filterMetricPatterns)
	receivedSamples.Add(float64(len(samples)))
	customerProfile.HyperpilotWriter.Put(samples)
}

func (server *Server) read(w http.ResponseWriter, r *http.Request) {
	tokenId := r.FormValue("tokenId")
	customerProfile, err := server.getCustomerProfile(tokenId)
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

func buildClients(server *Server, cp *CustomerProfile, remoteTimeout time.Duration) error {
	if cp.Config.InfluxdbURL != "" {
		url, err := url.Parse(cp.Config.InfluxdbURL)
		if err != nil {
			return fmt.Errorf("Failed to parse InfluxDB URL %s: %s", cp.Config.InfluxdbURL, err.Error())
		}
		conf := influx.HTTPConfig{
			Addr:     url.String(),
			Username: cp.Config.InfluxdbUsername,
			Password: cp.Config.InfluxdbPassword,
			Timeout:  remoteTimeout,
		}
		c := influxdb.NewClient(
			conf,
			cp.Config.InfluxdbDatabase,
			cp.Config.InfluxdbRetentionPolicy,
		)
		prometheus.MustRegister(c)
		cp.writer = c
		cp.reader = c

		if err := server.CreateWriter(cp); err != nil {
			return fmt.Errorf("Unable to create writer {%s}: %s", cp.Config.CustomerId, err.Error())
		}
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
