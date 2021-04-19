package stubs

import (
	"time"

	"github.com/vtex/go-io/redis"
	"github.com/vtex/go-io/reflext"
)

func NewRedis() redis.Cache {
	return &stubRedis{}
}

type stubRedis struct{}

func (r *stubRedis) Get(key string, result interface{}) (bool, error) {
	return false, nil
}

func (r *stubRedis) Exists(key string) (bool, error) {
	return false, nil
}

func (r *stubRedis) Set(key string, value interface{}, expireIn time.Duration) error {
	return nil
}

func (c *stubRedis) GetOrSet(key string, result interface{}, duration time.Duration, fetch func() (interface{}, error)) error {
	value, err := fetch()
	if err != nil {
		return err
	}

	err = reflext.SetPointer(result, value)
	if err != nil {
		return err
	}

	return nil
}

func (c *stubRedis) SetOpt(key string, value interface{}, options redis.SetOptions) (bool, error) {
	return true, nil
}

func (c *stubRedis) Del(key string) error {
	return nil
}

func (c *stubRedis) Incr(key string) (int64, error) {
	return 0, nil
}
