package models

type CustomerConfig struct {
	Token     string `bson:"token" json:"token"`
	ClusterId string `bson:"clusterId" json:"clusterId"`
}
