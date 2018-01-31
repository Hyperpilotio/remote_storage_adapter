package models

type StorageClientConfig struct {
	GraphiteAddress         string `bson:"graphiteAddress" json:"graphiteAddress"`
	GraphiteTransport       string `bson:"graphiteTransport" json:"graphiteTransport"`
	GraphitePrefix          string `bson:"graphitePrefix" json:"graphitePrefix"`
	OpentsdbURL             string `bson:"opentsdbURL" json:"opentsdbURL"`
	InfluxdbURL             string `bson:"influxdbURL" json:"influxdbURL"`
	InfluxdbRetentionPolicy string `bson:"influxdbRetentionPolicy" json:"influxdbRetentionPolicy"`
	InfluxdbUsername        string `bson:"clustinfluxdbUsernameerId" json:"influxdbUsername"`
	InfluxdbDatabase        string `bson:"influxdbDatabase" json:"influxdbDatabase"`
	InfluxdbPassword        string `bson:"influxdbPassword" json:"influxdbPassword"`
}

type CustomerConfig struct {
	Token                  string `bson:"token" json:"token"`
	CustomerName           string `bson:"clusterId" json:"clusterId"`
	ClusterId              string `bson:"clusterId" json:"clusterId"`
	FilterMetricsConfigURL string `bson:"filterMetricsConfigURL" json:"filterMetricsConfigURL"`

	StorageClientConfig
}
