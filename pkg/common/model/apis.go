package model

type CustomerConfig struct {
	Token      string `bson:"token" json:"token"`
	CustomerId string `bson:"customerId" json:"customerId"`
	ClusterId  string `bson:"clusterId" json:"clusterId"`
}
