package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"time"

	redisCluster "github.com/go-redis/redis/v8"
	"github.com/gomodule/redigo/redis"
	"github.com/pkg/errors"
	"github.com/vtex/go-io/cache"
	"github.com/vtex/go-io/reflext"
)

const (
	maxRedisCacheDuration = 12 * time.Hour
)

type TimeTracker func(kpiName string, startTime time.Time)

type RedisConfig struct {
	Endpoint       string
	ClusterMode    bool
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
	cache.Cache
	Exists(key string) (bool, error)
	SetOpt(key string, value interface{}, options SetOptions) (bool, error)
	Del(key string) error
	Incr(key string) (int64, error)
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

	if conf.ClusterMode {
		cluster := redisCluster.NewClusterClient(
			&redisCluster.ClusterOptions{
				Addrs:        []string{conf.Endpoint},
				ReadTimeout:  200 * time.Millisecond,
				WriteTimeout: 200 * time.Millisecond,
				DialTimeout:  1 * time.Second,
			},
		)

		return &redisC{cluster: cluster, conf: conf}
	}

	pool := newRedisPool(conf.Endpoint, poolOptions{
		MaxIdle:        conf.MaxIdleConns,
		MaxActive:      conf.MaxActiveConns,
		SetReadTimeout: true,
	})
	return &redisC{pool: pool, conf: conf}
}

type redisC struct {
	pool    *redis.Pool
	cluster *redisCluster.ClusterClient
	conf    RedisConfig
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

func (r *redisC) Incr(key string) (int64, error) {
	key, err := r.remoteKey(key)
	if err != nil {
		return 0, err
	}

	val, err := redis.Int64(r.doCmd("INCR", key))
	if err != nil {
		return 0, errors.WithStack(err)
	}

	return val, nil
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

	cacheDuration := minDuration(options.ExpireIn, maxRedisCacheDuration)
	args := []interface{}{key, bytes, "EX", int(cacheDuration.Seconds())}
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
	return reflext.SetPointer(result, value)
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

func (r *redisC) remoteKey(key string) (string, error) {
	if key == "" {
		return "", errors.Errorf("Cache key must not be empty (namespace: %s)", r.conf.KeyNamespace)
	}
	return r.conf.KeyNamespace + ":" + key, nil
}

func (r *redisC) doCmd(cmd string, args ...interface{}) (interface{}, error) {
	if r.cluster != nil {
		defer r.conf.TimeTracker(commandKpiName(cmd), time.Now())
		result, err := r.cluster.Do(context.Background(), append([]interface{}{cmd}, args...)...).Result()
		if err == redisCluster.Nil {
			return result, redis.ErrNil
		}

		return result, err
	}

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

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
