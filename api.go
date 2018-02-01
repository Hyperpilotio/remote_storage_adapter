package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/gobwas/glob"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/spf13/viper"

	influx "github.com/influxdata/influxdb/client/v2"

	"github.com/hyperpilotio/remote_storage_adapter/clients/influxdb"
	"github.com/hyperpilotio/remote_storage_adapter/db"
	"github.com/hyperpilotio/remote_storage_adapter/models"
	"github.com/hyperpilotio/remote_storage_adapter/prometheus/common/promlog"
)

// Server store the stats / data of every deployment
type Server struct {
	Config *viper.Viper
	AuthDB *db.AuthDB
	Logger log.Logger

	CustomerProfiles    map[string]*CustomerProfile
	customerProfileLock sync.RWMutex
}

// NewServer return an instance of Server struct.
func NewServer(cfg *viper.Viper) *Server {
	logLevel := promlog.AllowedLevel{}
	logLevel.Set("debug")
	return &Server{
		Config:           cfg,
		AuthDB:           db.NewAuthDB(cfg),
		Logger:           promlog.New(logLevel),
		CustomerProfiles: make(map[string]*CustomerProfile),
	}
}

// StartServer start a web servers
func (server *Server) StartServer() error {
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
			writers:              make([]writer, 0),
			readers:              make([]reader, 0),
			filterMetricPatterns: filterMetricPatterns,
		}

		if err := customerProfile.buildClients(server.Logger, remoteTimeout); err != nil {
			return fmt.Errorf("Unable build customer clients %s: %s", customerCfg.CustomerName, err.Error())
		}
		server.CustomerProfiles[customerCfg.Token] = customerProfile
	}

	http.Handle(server.Config.GetString("telemetryPath"), prometheus.Handler())
	http.HandleFunc("/write", server.write)
	http.HandleFunc("/read", server.read)

	level.Info(server.Logger).Log("Starting up...")
	return http.ListenAndServe(":"+server.Config.GetString("listenAddr"), nil)
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
	Config               *models.CustomerConfig
	writers              []writer
	readers              []reader
	filterMetricPatterns []glob.Glob
}

func (cp *CustomerProfile) buildClients(logger log.Logger, remoteTimeout time.Duration) error {
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
			log.With(logger, "storage", "InfluxDB"),
			conf,
			cp.Config.InfluxdbDatabase,
			cp.Config.InfluxdbRetentionPolicy,
		)
		prometheus.MustRegister(c)
		cp.writers = append(cp.writers, c)
		cp.readers = append(cp.readers, c)
	}
	return nil
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
		level.Error(server.Logger).Log("msg", "CustomerProfile not found: "+err.Error())
		http.Error(w, "CustomerProfile not found", http.StatusInternalServerError)
		return
	}

	compressed, err := ioutil.ReadAll(r.Body)
	if err != nil {
		level.Error(server.Logger).Log("msg", "Read error", "err", err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		level.Error(server.Logger).Log("msg", "Decode error", "err", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var req prompb.WriteRequest
	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		level.Error(server.Logger).Log("msg", "Unmarshal error", "err", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	samples := protoToSamples(&req, customerProfile.filterMetricPatterns)
	receivedSamples.Add(float64(len(samples)))

	var wg sync.WaitGroup
	for _, w := range customerProfile.writers {
		wg.Add(1)
		go func(rw writer) {
			sendSamples(server.Logger, rw, samples)
			wg.Done()
		}(w)
	}
	wg.Wait()
}

func (server *Server) read(w http.ResponseWriter, r *http.Request) {
	tokenId := r.FormValue("tokenId")
	customerProfile, err := server.getCustomerProfile(tokenId)
	if err != nil {
		level.Error(server.Logger).Log("msg", "CustomerProfile not found: "+err.Error())
		http.Error(w, "CustomerProfile not found", http.StatusInternalServerError)
		return
	}

	compressed, err := ioutil.ReadAll(r.Body)
	if err != nil {
		level.Error(server.Logger).Log("msg", "Read error", "err", err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		level.Error(server.Logger).Log("msg", "Decode error", "err", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var req prompb.ReadRequest
	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		level.Error(server.Logger).Log("msg", "Unmarshal error", "err", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// TODO: Support reading from more than one reader and merging the results.
	if len(customerProfile.readers) != 1 {
		http.Error(w, fmt.Sprintf("expected exactly one reader, found %d readers", len(customerProfile.readers)), http.StatusInternalServerError)
		return
	}
	reader := customerProfile.readers[0]

	var resp *prompb.ReadResponse
	resp, err = reader.Read(&req)
	if err != nil {
		level.Warn(server.Logger).Log("msg", "Error executing query", "query", req, "storage", reader.Name(), "err", err)
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

func sendSamples(logger log.Logger, w writer, samples model.Samples) {
	begin := time.Now()
	err := w.Write(samples)
	duration := time.Since(begin).Seconds()
	if err != nil {
		level.Warn(logger).Log("msg", "Error sending samples to remote storage", "err", err, "storage", w.Name(), "num_samples", len(samples))
		failedSamples.WithLabelValues(w.Name()).Add(float64(len(samples)))
	}
	sentSamples.WithLabelValues(w.Name()).Add(float64(len(samples)))
	sentBatchDuration.WithLabelValues(w.Name()).Observe(duration)
}
