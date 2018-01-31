package db

import (
	"errors"
	"fmt"

	"gopkg.in/mgo.v2/bson"

	"github.com/golang/glog"
	"github.com/hyperpilotio/remote_storage_adapter/models"
	"github.com/spf13/viper"
)

type AuthDB struct {
	Url                string
	User               string
	Password           string
	Database           string
	CustomerCollection string
}

func NewAuthDB(config *viper.Viper) *AuthDB {
	return &AuthDB{
		Url:                config.GetString("database.url"),
		User:               config.GetString("database.user"),
		Password:           config.GetString("database.password"),
		Database:           config.GetString("database.configDatabase"),
		CustomerCollection: config.GetString("database.customerCollection"),
	}
}

func connectMongo(url string, database string, user string, password string) (*mgo.Session, error) {
	dialInfo := &mgo.DialInfo{
		Addrs:    []string{url},
		Database: database,
		Username: user,
		Password: password,
	}
	session, sessionErr := mgo.DialWithInfo(dialInfo)
	if sessionErr != nil {
		return nil, errors.New("Unable to create mongo session: " + sessionErr.Error())
	}

	return session, nil
}

func (authDB *AuthDB) GetCustomerConfig(token string) (*models.CustomerConfig, error) {
	session, sessionErr := connectMongo(configDb.Url, configDb.Database, configDb.User, configDb.Password)
	if sessionErr != nil {
		return nil, errors.New("Unable to create mongo session: " + sessionErr.Error())
	}
	glog.V(1).Infof("Successfully connected to the config DB for app %s", name)
	defer session.Close()

	collection := session.DB(authDB.Database).C(authDB.CustomerCollection)
	var customerConfig models.CustomerConfig
	if err := collection.Find(bson.M{"token": token}).One(&customerConfig); err != nil {
		return nil, errors.New("Unable to find customer config from db: " + err.Error())
	}

	return &customerConfig, nil
}

func (authDB *AuthDB) getCollection(dataType string) (string, error) {
	switch dataType {
	case "customer":
		return authDB.CustomerCollection, nil
	default:
		return "", errors.New("Unable to find collection for: " + dataType)
	}
}

func (authDB *AuthDB) WriteMetrics(dataType string, obj interface{}) error {
	collectionName, collectionErr := authDB.getCollection(dataType)
	if collectionErr != nil {
		return collectionErr
	}

	session, sessionErr := connectMongo(authDB.Url, authDB.Database, authDB.User, authDB.Password)
	if sessionErr != nil {
		return errors.New("Unable to create mongo session: " + sessionErr.Error())
	}

	defer session.Close()

	collection := session.DB(authDB.Database).C(collectionName)
	if err := collection.Insert(obj); err != nil {
		return errors.New("Unable to insert into collection: " + err.Error())
	}

	return nil
}

func (authDB *AuthDB) UpsertMetrics(dataType string, appName string, obj interface{}) error {
	collectionName, collectionErr := authDB.getCollection(dataType)
	if collectionErr != nil {
		return collectionErr
	}

	session, sessionErr := connectMongo(authDB.Url, authDB.Database, authDB.User, authDB.Password)
	if sessionErr != nil {
		return errors.New("Unable to create mongo session: " + sessionErr.Error())
	}

	defer session.Close()

	collection := session.DB(authDB.Database).C(collectionName)
	if _, err := collection.Upsert(bson.M{"appName": appName}, obj); err != nil {
		return fmt.Errorf("Unable to upsert %s into metrics db: %s", dataType, err.Error())
	}

	return nil
}
