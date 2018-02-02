package db

import (
	"testing"

	hpmodel "github.com/hyperpilotio/remote_storage_adapter/pkg/common/model"
	"github.com/spf13/viper"
)

func TestAuthDB(t *testing.T) {
	config := viper.New()
	config.SetConfigType("json")
	config.Set("database.url", "localhost:27017")
	config.Set("database.user", "analyzer")
	config.Set("database.password", "hyperpilot")
	config.Set("database.configDatabase", "authdb")
	config.Set("database.customerCollection", "customers")
	authdb := NewAuthDB(config)

	customerConfig := &hpmodel.CustomerConfig{
		Token:      "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9",
		CustomerId: "hyperpilotio",
		ClusterId:  "test",
		InfluxdbClientConfig: hpmodel.InfluxdbClientConfig{
			InfluxdbURL:             "localhost",
			InfluxdbRetentionPolicy: "autogen",
			InfluxdbUsername:        "root",
			InfluxdbPassword:        "default",
			InfluxdbDatabase:        "prometheus",
		},
	}
	if err := authdb.WriteMetrics("customer", customerConfig); err != nil {
		t.Error("Unable write customer data to mongo")
	}
}
