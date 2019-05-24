package cache

import (
	"time"

	gocache "github.com/pmylund/go-cache"
	"github.com/vtex/go-io/util"
)

func NewMemory() Cache {
	return &memCache{gocache.New(60*time.Minute, 10*time.Minute)}
}

type memCache struct {
	cache *gocache.Cache
}

func (c *memCache) GetOrSet(key string, result interface{}, duration time.Duration, fetch func() (interface{}, error)) error {
	if err := ensureValidCacheKey(key); err != nil {
		return err
	}

	if cached, _ := c.Get(key, result); cached {
		return nil
	}

	value, err := fetch()
	if err != nil {
		return err
	}

	c.Set(key, value, duration)
	return util.SetPointer(result, value)
}

func (c *memCache) Get(key string, result interface{}) (bool, error) {
	value, cached := c.cache.Get(key)
	if !cached {
		return false, nil
	}

	return true, util.SetPointer(result, value)
}

func (c *memCache) Set(key string, value interface{}, duration time.Duration) error {
	c.cache.Set(key, value, duration)
	return nil
}
