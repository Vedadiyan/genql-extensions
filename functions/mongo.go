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
	"go.mongodb.org/mongo-driver/bson/primitive"
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
	doc := bson.A{}
	err = cursor.All(context.TODO(), &doc)
	if err != nil {
		return nil, err
	}
	rs := Unmarshal(doc)
	return rs, nil
}

func Neutralize(any any) any {
	switch any := any.(type) {
	case int:
		{
			return float64(any)
		}
	case int32:
		{
			return float64(any)

		}
	case int64:
		{
			return float64(any)
		}
	case int16:
		{
			return float64(any)
		}
	case int8:
		{
			return float64(any)
		}
	case byte:
		{
			return float64(any)
		}
	case float32:
		{
			return float64(any)
		}
	default:
		{
			return any
		}
	}
}

func UnmarshalArray(data bson.A) any {
	slice := make([]any, 0)
	for _, value := range data {
		slice = append(slice, Unmarshal(value))
	}
	return slice
}

func UnmarshalMap(data bson.M) any {
	mapper := make(map[string]any)
	for key, value := range data {
		mapper[key] = Unmarshal(value)
	}
	return mapper
}
func UnmarshalDocument(document bson.D) any {
	mapper := make(map[string]any)
	for _, item := range document {
		mapper[item.Key] = Unmarshal(item.Value)
	}
	return mapper
}

func Unmarshal(any any) any {
	switch any := any.(type) {
	case bson.D:
		{
			return UnmarshalDocument(any)
		}
	case bson.M:
		{
			return UnmarshalMap(any)
		}
	case bson.A:
		{
			return UnmarshalArray(any)
		}
	case primitive.ObjectID:
		{
			return any.String()
		}
	default:
		{
			return Neutralize(any)
		}
	}
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
