package model

type Organization struct {
	OrgId     string `bson:"orgId" json:"orgId"`
	Token     string `bson:"token" json:"token"`
	ClusterId string `bson:"clusterId" json:"clusterId"`
}

func (org *Organization) InfluxUrl() string {
	return "http://influxdb-" + org.OrgId + ":8086"
}
