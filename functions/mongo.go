package functions

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"text/template"

	"github.com/vedadiyan/genql-extensions/sentinel"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type (
	MongoArgs struct {
		ConnectionName string
		Database       string
		Collection     string
		Query          string
		Args           any
	}
)

var (
	_mongoArgsFieldCount int
	_mongoConnections    map[string]*mongo.Client
	_mongoConnectionLock sync.RWMutex
)

func init() {
	_mongoArgsFieldCount = reflect.TypeOf(MongoArgs{}).NumField()
	_mongoConnections = make(map[string]*mongo.Client)
}

func MongoFunc(args []any) (any, error) {
	mongoArgs, err := parseMongoArgs(args)
	if err != nil {
		return nil, err
	}
	template, err := template.New("tmp").Parse(mongoArgs.Query)
	if err != nil {
		return nil, err
	}
	var queryRaw bytes.Buffer
	err = template.Execute(&queryRaw, mongoArgs.Args)
	if err != nil {
		return nil, err
	}
	_mongoConnectionLock.RLock()
	connection, ok := _mongoConnections[mongoArgs.ConnectionName]
	_mongoConnectionLock.RUnlock()
	if !ok {
		return nil, fmt.Errorf("the given connection `%s` has not been registered", mongoArgs.ConnectionName)
	}
	database := connection.Database(mongoArgs.Database)
	collection := database.Collection(mongoArgs.Collection)
	var query bson.A
	err = json.Unmarshal(queryRaw.Bytes(), &query)
	if err != nil {
		return nil, err
	}
	cursor, err := collection.Aggregate(context.TODO(), query)
	if err != nil {
		return nil, err
	}
	rs := make([]any, 0)
	for cursor.Next(context.TODO()) {
		data := make(map[string]any)
		err := json.Unmarshal([]byte(cursor.Current.String()), &data)
		if err != nil {
			return nil, err
		}
		rs = append(rs, data)
	}
	return rs, nil
}

func parseMongoArgs(rawArg []any) (*MongoArgs, error) {
	if len(rawArg) > _mongoArgsFieldCount {
		return nil, sentinel.TOO_MANY_ARGS
	}
	if len(rawArg) < _mongoArgsFieldCount {
		return nil, sentinel.TOO_FEW_ARGS
	}
	connection, ok := rawArg[0].(string)
	if !ok {
		return nil, fmt.Errorf("expected string but found %T", rawArg[0])
	}
	database, ok := rawArg[1].(string)
	if !ok {
		return nil, fmt.Errorf("expected string but found %T", rawArg[1])
	}
	collection, ok := rawArg[2].(string)
	if !ok {
		return nil, fmt.Errorf("expected string but found %T", rawArg[2])
	}
	query, ok := rawArg[3].(string)
	if !ok {
		return nil, fmt.Errorf("expected string but found %T", rawArg[3])
	}
	mongoArgs := MongoArgs{
		ConnectionName: connection,
		Database:       database,
		Collection:     collection,
		Query:          query,
		Args:           rawArg[4],
	}
	return &mongoArgs, nil
}

func RegisterMongoConnection(name string, instanceCreator func() (*mongo.Client, error)) error {
	instance, err := instanceCreator()
	if err != nil {
		return err
	}
	_mongoConnectionLock.Lock()
	_mongoConnections[name] = instance
	_mongoConnectionLock.Unlock()
	return nil
}
