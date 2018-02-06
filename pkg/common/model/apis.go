package model

type CustomerConfig struct {
	Token                   string `bson:"token" json:"token"`
	CustomerId              string `bson:"customerId" json:"customerId"`
	ClusterId               string `bson:"clusterId" json:"clusterId"`
	InfluxdbURL             string `bson:"influxdbURL" json:"influxdbURL"`
	InfluxdbRetentionPolicy string `bson:"influxdbRetentionPolicy" json:"influxdbRetentionPolicy"`
	InfluxdbUsername        string `bson:"influxdbUsername" json:"influxdbUsername"`
	InfluxdbDatabase        string `bson:"influxdbDatabase" json:"influxdbDatabase"`
	InfluxdbPassword        string `bson:"influxdbPassword" json:"influxdbPassword"`
}
