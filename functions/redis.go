package functions

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/vedadiyan/genql-extensions/sentinel"
)

type (
	RedisArgs struct {
		ConnectionName string
		Command        string
		Arg            any
		Key            string `optional:"true" position:"3"`
		TTL            int64  `optional:"true" position:"4"`
	}
)

var (
	_redisArgsFieldCount int
	_redisOptionalFields map[string]int
	_redisConnections    map[string]*redis.Client
	_redisConnectionLock sync.RWMutex
	_dictionary          = []rune{
		'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
		'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
		'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
		'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
		'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '_', '-',
	}
)

func init() {
	fields, optionalFields, err := ArgStructAnalyzer[RedisArgs]()
	if err != nil {
		panic(err)
	}
	_redisArgsFieldCount = fields
	_redisOptionalFields = optionalFields
	_redisConnections = make(map[string]*redis.Client)
}

func RedisFunc(args []any) (any, error) {
	redisArgs, err := parseRedisArgs(args)
	if err != nil {
		return nil, err
	}
	_redisConnectionLock.RLock()
	connection, ok := _redisConnections[redisArgs.ConnectionName]
	_redisConnectionLock.RUnlock()
	if !ok {
		return nil, fmt.Errorf("the given connection `%s` has not been registered", redisArgs.ConnectionName)
	}
	switch strings.ToLower(redisArgs.Command) {
	case "cache":
		{
			return redisCache(connection, redisArgs.Arg, time.Second*time.Duration(redisArgs.TTL))
		}
	case "set":
		{
			return redisSet(connection, redisArgs.Key, redisArgs.Arg, time.Second*time.Duration(redisArgs.TTL))
		}
	case "changekey":
		{
			return redisChangeKey(connection, redisArgs.Key, redisArgs.Arg, time.Second*time.Duration(redisArgs.TTL))
		}
	case "get":
		{
			return redisGet(connection, redisArgs.Key)
		}
	case "incr":
		{
			return redisIncr(connection, redisArgs.Key)
		}
	case "incr/base64":
		{
			rs, err := redisIncr(connection, redisArgs.Key)
			if err != nil {
				return nil, err
			}
			return toBase64(uint64(rs.(int64))), nil
		}
	default:
		{
			return nil, fmt.Errorf("the given command `%s` is not a valid command", redisArgs.Command)
		}
	}
}

func redisCache(conn *redis.Client, args any, ttl time.Duration) (string, error) {
	json, err := json.Marshal(map[string]any{"data": args})
	if err != nil {
		return "", err
	}
	uuid := uuid.NewString()
	rs := conn.Set(context.TODO(), uuid, json, ttl)
	if rs.Err() != nil {
		return "", rs.Err()
	}
	return uuid, nil
}

func redisSet(conn *redis.Client, key string, args any, ttl time.Duration) (any, error) {
	json, err := json.Marshal(map[string]any{"data": args})
	if err != nil {
		return "", err
	}
	rs := conn.Set(context.TODO(), key, json, ttl)
	if rs.Err() != nil {
		return "", rs.Err()
	}
	return args, nil
}

func redisChangeKey(conn *redis.Client, key string, args any, ttl time.Duration) (any, error) {
	newKey, ok := args.(string)
	if !ok {
		return nil, fmt.Errorf("expected string but found %T", args)
	}
	value, err := conn.Get(context.TODO(), key).Result()
	if err != nil {
		return nil, err
	}
	_, err = conn.Del(context.TODO(), key).Result()
	if err != nil {
		return nil, err
	}
	_, err = conn.Set(context.TODO(), newKey, value, ttl).Result()
	if err != nil {
		return nil, err
	}
	return value, nil
}

func redisGet(conn *redis.Client, key string) (any, error) {
	rs := conn.Get(context.TODO(), key)
	if rs.Err() != nil {
		return "", rs.Err()
	}
	data := make(map[string]any)
	err := json.Unmarshal([]byte(rs.Val()), &data)
	if err != nil {
		return "", err
	}
	return data["data"], nil
}

func redisIncr(conn *redis.Client, key string) (any, error) {
	exists := conn.Exists(context.TODO(), key)
	if exists.Err() != nil {
		return nil, exists.Err()
	}
	if exists.Val() == 0 {
		rs := conn.Set(context.TODO(), key, 0, 0)
		if rs.Err() != nil {
			return nil, rs.Err()
		}
	}
	rs := conn.Incr(context.TODO(), key)
	if rs.Err() != nil {
		return "", rs.Err()
	}
	return rs.Val(), nil
}

func parseRedisArgs(rawArg []any) (*RedisArgs, error) {
	len := len(rawArg)
	if len < _redisArgsFieldCount {
		return nil, sentinel.TOO_FEW_ARGS
	}
	connection, ok := rawArg[0].(string)
	if !ok {
		return nil, fmt.Errorf("expected string but found %T", rawArg[0])
	}
	command, ok := rawArg[1].(string)
	if !ok {
		return nil, fmt.Errorf("expected string but found %T", rawArg[0])
	}
	redisArgs := RedisArgs{
		ConnectionName: connection,
		Command:        command,
		Arg:            rawArg[2],
	}
	for key, value := range _redisOptionalFields {
		if rawArg[value] == nil {
			continue
		}
		if len <= value {
			continue
		}
		switch key {
		case "Key":
			{
				key, ok := rawArg[value].(string)
				if !ok {
					return nil, fmt.Errorf("expected string but found %T", rawArg[value])
				}
				redisArgs.Key = key
			}
		case "TTL":
			{
				ttl, ok := rawArg[value].(float64)
				if !ok {
					return nil, fmt.Errorf("expected int64 but found %T", rawArg[value])
				}
				redisArgs.TTL = int64(ttl)
			}
		}
	}
	return &redisArgs, nil
}

func toBase64(n uint64) string {
	block := make([]string, 0)
	for n != 0 {
		block = append(block, string(_dictionary[n%64]))
		n /= 64
	}
	return strings.Join(block, "")
}

func RegisterRedisConnection(name string, instanceCreator func() (*redis.Client, error)) error {
	instance, err := instanceCreator()
	if err != nil {
		return err
	}
	_redisConnectionLock.Lock()
	_redisConnections[name] = instance
	_redisConnectionLock.Unlock()
	return nil
}
