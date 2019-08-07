package redis

import (
	"encoding/json"
	"fmt"
	"strings"

	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/pkg/errors"
	"github.com/vtex/go-io/util"
)

type TimeTracker func(kpiName string, startTime time.Time)

type RedisConfig struct {
	Endpoint       string
	KeyNamespace   string
	TimeTracker    TimeTracker
	MaxIdleConns   int
	MaxActiveConns int
}

type SetOptions struct {
	ExpireIn   time.Duration
	IfNotExist bool
}

type Cache interface {
	Get(key string, result interface{}) (bool, error)
	Exists(key string) (bool, error)
	Set(key string, value interface{}, expireIn time.Duration) error
	SetOpt(key string, value interface{}, options SetOptions) (bool, error)
	GetOrSet(key string, result interface{}, expireIn time.Duration, fetch func() (interface{}, error)) error
	Del(key string) error
	DeleteMatching(pattern string) error
}

func New(conf RedisConfig) Cache {
	if conf.TimeTracker == nil {
		conf.TimeTracker = func(string, time.Time) {}
	}
	if conf.MaxIdleConns == 0 {
		conf.MaxIdleConns = 10
	}
	if conf.MaxActiveConns == 0 {
		conf.MaxActiveConns = 20
	}

	pool := newRedisPool(conf.Endpoint, poolOptions{
		MaxIdle:        conf.MaxIdleConns,
		MaxActive:      conf.MaxActiveConns,
		SetReadTimeout: true,
	})
	return &redisC{pool: pool, conf: conf}
}

type redisC struct {
	pool *redis.Pool
	conf RedisConfig
}

func (r *redisC) Get(key string, result interface{}) (bool, error) {
	key, err := r.remoteKey(key)
	if err != nil {
		return false, err
	}

	reply, err := redis.Bytes(r.doCmd("GET", key))
	if err == redis.ErrNil {
		return false, nil
	} else if err != nil {
		return false, errors.WithStack(err)
	}

	if bytesRes, isBytesPtr := result.(*[]byte); isBytesPtr {
		*bytesRes = reply
		return true, nil
	} else if err := json.Unmarshal(reply, result); err != nil {
		return false, errors.Wrap(err, "Failed to umarshal Redis response")
	}
	return true, nil
}

func (r *redisC) Exists(key string) (bool, error) {
	key, err := r.remoteKey(key)
	if err != nil {
		return false, err
	}

	exists, err := redis.Bool(r.doCmd("EXISTS", key))
	if err != nil {
		return false, errors.WithStack(err)
	}
	return exists, nil
}

func (r *redisC) Set(key string, value interface{}, expireIn time.Duration) error {
	_, err := r.SetOpt(key, value, SetOptions{ExpireIn: expireIn})
	return err
}

func (r *redisC) SetOpt(key string, value interface{}, options SetOptions) (bool, error) {
	key, err := r.remoteKey(key)
	if err != nil {
		return false, err
	}

	bytes, isBytes := value.([]byte)
	if !isBytes {
		bytes, err = json.Marshal(value)
		if err != nil {
			return false, errors.Wrap(err, "Failed to marshal value for saving to Redis")
		}
	}

	args := []interface{}{key, bytes, "EX", int(options.ExpireIn.Seconds())}
	if options.IfNotExist {
		args = append(args, "NX")
	}

	res, err := r.doCmd("SET", args...)
	if err != nil {
		return false, errors.Wrap(err, "Failed SET command on Redis")
	}
	return res != nil, nil
}

func (r *redisC) GetOrSet(key string, result interface{}, expireIn time.Duration, fetch func() (interface{}, error)) error {
	if ok, err := r.Get(key, result); ok {
		return nil
	} else if err != nil {
		logError(err, "redis_cache_get_error", r.conf.KeyNamespace, key, "Error getting data from redis")
	}

	value, err := fetch()
	if err != nil {
		return err
	}

	r.Set(key, value, expireIn)
	return util.SetPointer(result, value)
}

func (r *redisC) Del(key string) error {
	key, err := r.remoteKey(key)
	if err != nil {
		return err
	}

	if _, err := r.doCmd("DEL", key); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (r *redisC) DeleteMatching(pattern string) error {
	pattern, err := r.remoteKey(pattern)
	if err != nil {
		return err
	}

	// Use the same connection to run multiple commands.
	conn := r.getConnection()
	defer r.closeConnection(conn)

	var keys []interface{}

	// We will use SCAN to go through the keys we want to delete without blocking the Redis server.
	cursor := "0"
	for {
		cursor, keys, err = scanIteration(conn, cursor, pattern)
		if err != nil {
		}

		if len(keys) > 0 {
			if _, err := conn.Do("DEL", keys...); err != nil {
				return err
			}
		}

		if cursor == "0" {
			return nil
		}
	}
}

func (r *redisC) remoteKey(key string) (string, error) {
	if key == "" {
		return "", errors.Errorf("Cache key must not be empty (namespace: %s)", r.conf.KeyNamespace)
	}
	return r.conf.KeyNamespace + ":" + key, nil
}

func (r *redisC) doCmd(cmd string, args ...interface{}) (interface{}, error) {
	conn := r.getConnection()
	defer r.closeConnection(conn)

	defer r.conf.TimeTracker(commandKpiName(cmd), time.Now())
	return conn.Do(cmd, args...)
}

func (r *redisC) getConnection() redis.Conn {
	defer r.conf.TimeTracker("redis_get_connection", time.Now())
	return r.pool.Get()
}

func (r *redisC) closeConnection(conn redis.Conn) error {
	defer r.conf.TimeTracker("redis_close_connection", time.Now())
	return conn.Close()
}

func commandKpiName(cmd string) string {
	return fmt.Sprintf("redis_command_%s", strings.ToLower(cmd))
}

func scanIteration(conn redis.Conn, cursor, pattern string) (string, []interface{}, error) {
	reply, err := conn.Do("SCAN", cursor, "MATCH", pattern)
	if err != nil {
		return "", nil, err
	}

	// res is a slice that contains a []byte with a single element, which is the next cursor we need to use, and a
	// []interface{}, whose elements are []byte and each represent a key if when converted to string.
	res, err := redis.Values(reply, err)
	if err != nil {
		return "", nil, err
	}

	if len(res) != 2 {
		return "", nil, errors.Errorf("Invalid response from Redis. Expected array of length 2, but got length %d", len(res))
	}

	cursor, err = redis.String(res[0], err)
	if err != nil {
		return "", nil, errors.Wrap(err, "Unable to get cursor for next scan iteration")
	}

	keys, err := redis.Values(res[1], err)
	if err != nil {
		return "", nil, errors.Wrap(err, "Unable to get keys from scan iteration")
	}

	return cursor, keys, nil
}
