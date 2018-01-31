package main

import (
	"errors"
	"flag"
	"os"
	"time"

	"github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/viper"
)

var (
	receivedSamples = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "received_samples_total",
			Help: "Total number of received samples.",
		},
	)
	sentSamples = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "sent_samples_total",
			Help: "Total number of processed samples sent to remote storage.",
		},
		[]string{"remote"},
	)
	failedSamples = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "failed_samples_total",
			Help: "Total number of processed samples which failed on send to remote storage.",
		},
		[]string{"remote"},
	)
	sentBatchDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "sent_batch_duration_seconds",
			Help:    "Duration of sample batch send calls to the remote storage.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"remote"},
	)
)

func init() {
	prometheus.MustRegister(receivedSamples)
	prometheus.MustRegister(sentSamples)
	prometheus.MustRegister(failedSamples)
	prometheus.MustRegister(sentBatchDuration)
}

func parseFlags() *config {
	cfg := &config{
		influxdbPassword: os.Getenv("INFLUXDB_PW"),
	}

	flag.StringVar(&cfg.graphiteAddress, "graphite-address", "",
		"The host:port of the Graphite server to send samples to. None, if empty.",
	)
	flag.StringVar(&cfg.graphiteTransport, "graphite-transport", "tcp",
		"Transport protocol to use to communicate with Graphite. 'tcp', if empty.",
	)
	flag.StringVar(&cfg.graphitePrefix, "graphite-prefix", "",
		"The prefix to prepend to all metrics exported to Graphite. None, if empty.",
	)
	flag.StringVar(&cfg.opentsdbURL, "opentsdb-url", "",
		"The URL of the remote OpenTSDB server to send samples to. None, if empty.",
	)
	flag.StringVar(&cfg.influxdbURL, "influxdb-url", "",
		"The URL of the remote InfluxDB server to send samples to. None, if empty.",
	)
	flag.StringVar(&cfg.influxdbRetentionPolicy, "influxdb.retention-policy", "autogen",
		"The InfluxDB retention policy to use.",
	)
	flag.StringVar(&cfg.influxdbUsername, "influxdb.username", "",
		"The username to use when sending samples to InfluxDB. The corresponding password must be provided via the INFLUXDB_PW environment variable.",
	)
	flag.StringVar(&cfg.influxdbDatabase, "influxdb.database", "prometheus",
		"The name of the database to use for storing samples in InfluxDB.",
	)
	flag.DurationVar(&cfg.remoteTimeout, "send-timeout", 30*time.Second,
		"The timeout to use when sending samples to the remote storage.",
	)
	flag.StringVar(&cfg.listenAddr, "web.listen-address", ":9201", "Address to listen on for web endpoints.")
	flag.StringVar(&cfg.telemetryPath, "web.telemetry-path", "/metrics", "Address to listen on for web endpoints.")
	flag.StringVar(&cfg.storageAdapterConfigPath, "storageAdapterConfigPath", "/etc/storage_adapter/adapter_config.json",
		"The path of the storage adapter config.",
	)

	flag.Parse()

	return cfg
}

func readConfig(fileConfig string) (*viper.Viper, error) {
	viper := viper.New()
	viper.SetConfigType("json")

	if fileConfig == "" {
		viper.SetConfigName("adapter_config")
		viper.AddConfigPath("/etc/storage_adapter")
	} else {
		viper.SetConfigFile(fileConfig)
	}

	// overwrite by file
	err := viper.ReadInConfig()
	if err != nil {
		return nil, err
	}

	return viper, nil
}

// Run start the web server
func Run(cfg *config) error {
	adapterCfg, err := readConfig(cfg.storageAdapterConfigPath)
	if err != nil {
		return errors.New("Unable to read configure file: " + err.Error())
	}

	server := NewServer(cfg, adapterCfg)
	return server.StartServer()
}

func main() {
	cfg := parseFlags()
	err := Run(cfg)
	glog.Errorln(err)
}
