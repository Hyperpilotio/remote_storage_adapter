package db

import (
	"errors"
	"fmt"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/hyperpilotio/remote_storage_adapter/pkg/common/model"
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

func (authDB *AuthDB) GetCustomers() ([]model.CustomerConfig, error) {
	session, sessionErr := connectMongo(authDB.Url, authDB.Database, authDB.User, authDB.Password)
	if sessionErr != nil {
		return nil, errors.New("Unable to create mongo session: " + sessionErr.Error())
	}
	defer session.Close()

	var customers []model.CustomerConfig
	collection := session.DB(authDB.Database).C(authDB.CustomerCollection)
	if err := collection.Find(nil).All(&customers); err != nil {
		return nil, errors.New("Unable to read customers from auth db: " + err.Error())
	}

	return customers, nil
}

func (authDB *AuthDB) GetOneCustomerByToken(token string) (*model.CustomerConfig, error) {
	session, sessionErr := connectMongo(authDB.Url, authDB.Database, authDB.User, authDB.Password)
	if sessionErr != nil {
		return nil, errors.New("Unable to create mongo session: " + sessionErr.Error())
	}
	defer session.Close()

	var customers model.CustomerConfig
	collection := session.DB(authDB.Database).C(authDB.CustomerCollection)

	if err := collection.Find(bson.M{"token": token}).One(&customers); err != nil {
		return nil, errors.New(
			fmt.Sprintf("Unable to read customers with token %s from auth db: %s", token, err.Error()))
	}

	return &customers, nil
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
