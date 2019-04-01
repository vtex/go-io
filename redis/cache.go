package redis

import (
	"encoding/json"

	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/pkg/errors"
	"github.com/vtex/go-io/util"
)

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
}

func New(endpoint, keyNamespace string) Cache {
	pool := newRedisPool(endpoint, poolOptions{
		MaxIdle:        50,
		MaxActive:      100,
		SetReadTimeout: true,
	})
	return &redisC{pool: pool, keyNamespace: keyNamespace}
}

type redisC struct {
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
		return false, errors.Errorf("Redis response for command GET wasn't a []byte: %s", reply)
	} else if bytesRes, isBytesPtr := result.(*[]byte); isBytesPtr {
		*bytesRes = bytes
		return true, nil
	} else if err := json.Unmarshal(bytes, result); err != nil {
		return false, errors.Wrap(err, "Failed to umarshal Redis response")
	}
	return true, nil
}

func (r *redisC) Exists(key string) (bool, error) {
	key, err := remoteKey(r.keyNamespace, key)
	if err != nil {
		return false, err
	}

	reply, err := r.doCmd("EXISTS", key)
	if err != nil {
		return false, errors.WithStack(err)
	}

	keyExists, isBool := reply.(bool)
	if !isBool {
		return false, errors.Errorf("Redis response for command EXISTS wasn't a bool: %s", reply)
	}
	return keyExists, nil
}

func (r *redisC) Set(key string, value interface{}, expireIn time.Duration) error {
	_, err := r.SetOpt(key, value, SetOptions{ExpireIn: expireIn})
	return err
}

func (r *redisC) SetOpt(key string, value interface{}, options SetOptions) (bool, error) {
	key, err := remoteKey(r.keyNamespace, key)
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
		logError(err, "redis_cache_get_error", r.keyNamespace, key, "Error getting data from redis")
	}

	value, err := fetch()
	if err != nil {
		return err
	}

	r.Set(key, value, expireIn)
	return util.SetPointer(result, value)
}

func (r *redisC) Del(key string) error {
	key, err := remoteKey(r.keyNamespace, key)
	if err != nil {
		return err
	}

	if _, err := r.doCmd("DEL", key); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (r *redisC) doCmd(cmd string, args ...interface{}) (interface{}, error) {
	conn := r.pool.Get()
	defer conn.Close()
	reply, err := conn.Do(cmd, args...)
	return reply, err
}
