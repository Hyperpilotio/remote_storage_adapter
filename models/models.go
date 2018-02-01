package models

type InfluxdbClientConfig struct {
	InfluxdbURL             string `bson:"influxdbURL" json:"influxdbURL"`
	InfluxdbRetentionPolicy string `bson:"influxdbRetentionPolicy" json:"influxdbRetentionPolicy"`
	InfluxdbUsername        string `bson:"clustinfluxdbUsernameerId" json:"influxdbUsername"`
	InfluxdbDatabase        string `bson:"influxdbDatabase" json:"influxdbDatabase"`
	InfluxdbPassword        string `bson:"influxdbPassword" json:"influxdbPassword"`
}

type CustomerConfig struct {
	Token                  string `bson:"token" json:"token"`
	CustomerName           string `bson:"customerName" json:"customerName"`
	ClusterId              string `bson:"clusterId" json:"clusterId"`
	FilterMetricsConfigURL string `bson:"filterMetricsConfigURL" json:"filterMetricsConfigURL"`

	InfluxdbClientConfig
}
