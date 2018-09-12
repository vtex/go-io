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
	GetOrSet(key string, result interface{}, fetch func() (interface{}, error), duration time.Duration) error
}

func New(endpoint string) Redis {
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

	return &redisC{pool: pool}
}

type redisC struct {
	lock sync.RWMutex
	pool *redis.Pool
}

func (r *redisC) Get(key string, result interface{}) (bool, error) {
	reply, err := r.doCmd("GET", key)
	if err != nil {
		return false, errors.WithStack(err)
	} else if reply == nil {
		return false, nil
	}

	if bytes, ok := reply.([]byte); !ok {
		return false, errors.Errorf("Unrecognized Redis reply: %s", reply)
	} else if err := json.Unmarshal(bytes, result); err != nil {
		return false, errors.WithStack(err)
	}
	return true, nil
}

func (r *redisC) Set(key string, value interface{}, expireIn time.Duration) error {
	bytes, err := json.Marshal(value)
	if err != nil {
		return errors.WithStack(err)
	}

	if _, err := r.doCmd("SET", key, bytes, "EX", int(expireIn.Seconds())); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (r *redisC) doCmd(cmd string, args ...interface{}) (interface{}, error) {
	conn := r.pool.Get()
	reply, err := conn.Do(cmd, args...)
	conn.Close()
	return reply, err
}

func logError(err error, code, msg string) {
	logrus.WithError(err).WithField("code", code).Error(msg)
}

func (c *redisC) GetOrSet(key string, result interface{}, fetch func() (interface{}, error), duration time.Duration) error {
	if ok, err := c.Get(key, result); ok {
		return nil
	} else if err != nil {
		logError(err, "redis_cache_get_error", "Error getting data from redis")
	}

	value, err := fetch()
	if err != nil {
		return err
	}

	c.Set(key, value, duration)

	err = util.SetPointer(result, value)
	if err != nil {
		return err
	}

	return nil
}
