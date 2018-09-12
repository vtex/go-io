package redis

import (
	"encoding/json"
	"sync"

	"time"

	"github.com/Sirupsen/logrus"
	"github.com/garyburd/redigo/redis"
	"github.com/pkg/errors"
	"github.com/vtex/go-io/util"
)

const pingDelay = 60 * time.Second

type Redis interface {
	Get(key string, result interface{}) (bool, error)
	Set(key string, value interface{}, expireIn time.Duration) error
	GetOrSet(key string, result interface{}, duration time.Duration, fetch func() (interface{}, error)) error
}

func New(endpoint, keyNamespace string) Redis {
	pool := &redis.Pool{
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", endpoint, redis.DialConnectTimeout(1*time.Second))
		},
		TestOnBorrow: func(conn redis.Conn, idleSince time.Time) error {
			if time.Since(idleSince) < pingDelay {
				return nil
			}
			_, err := conn.Do("PING")
			return err
		},
		MaxIdle:     5,
		MaxActive:   10,
		Wait:        true,
		IdleTimeout: 3 * time.Minute,
	}

	return &redisC{pool: pool, keyNamespace: keyNamespace}
}

type redisC struct {
	lock         sync.RWMutex
	pool         *redis.Pool
	keyNamespace string
}

func (r *redisC) Get(key string, result interface{}) (bool, error) {
	key, err := remoteKey(r.keyNamespace, key)
	if err != nil {
		return false, err
	}

	reply, err := r.doCmd("GET", key)
	if err != nil {
		return false, errors.WithStack(err)
	} else if reply == nil {
		return false, nil
	}

	if bytes, ok := reply.([]byte); !ok {
		return false, errors.Errorf("Unrecognized Redis response: %s", reply)
	} else if err := json.Unmarshal(bytes, result); err != nil {
		return false, errors.Wrap(err, "Failed to umarshal Redis response")
	}
	return true, nil
}

func (r *redisC) Set(key string, value interface{}, expireIn time.Duration) error {
	key, err := remoteKey(r.keyNamespace, key)
	if err != nil {
		return err
	}

	bytes, err := json.Marshal(value)
	if err != nil {
		return errors.Wrap(err, "Failed to save data to Redis")
	}

	if _, err := r.doCmd("SET", key, bytes, "EX", int(expireIn.Seconds())); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (r *redisC) GetOrSet(key string, result interface{}, duration time.Duration, fetch func() (interface{}, error)) error {
	if ok, err := r.Get(key, result); ok {
		return nil
	} else if err != nil {
		logError(err, "redis_cache_get_error", r.keyNamespace, key, "Error getting data from redis")
	}

	value, err := fetch()
	if err != nil {
		return err
	}

	r.Set(key, value, duration)
	return util.SetPointer(result, value)
}

func (r *redisC) doCmd(cmd string, args ...interface{}) (interface{}, error) {
	conn := r.pool.Get()
	reply, err := conn.Do(cmd, args...)
	conn.Close()
	return reply, err
}

func logError(err error, code, namespace, key, msg string) {
	logrus.WithError(err).
		WithFields(logrus.Fields{
			"code":         code,
			"category":     "redis_cache",
			"keyNamespace": namespace,
			"key":          key,
		}).Error(msg)
}

func remoteKey(ns, key string) (string, error) {
	if key == "" {
		return "", errors.Errorf("Cache key must not be empty")
	}
	return ns + ":" + key, nil
}
