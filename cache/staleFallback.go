package cache

import (
	"encoding/json"
	"time"

	"github.com/pkg/errors"
	"github.com/vtex/go-io/reflext"
)

const (
	staleCacheLogCategory = "cache_with_stale_fallback"
)

func WithStaleFallback(storage Cache, staleTTL time.Duration) Stale {
	return &staleFallbackCache{
		cache:    storage,
		staleTTL: staleTTL,
	}
}

type staleFallbackCache struct {
	cache    Cache
	staleTTL time.Duration
}

func (c *staleFallbackCache) GetOrSet(key string, result interface{}, duration time.Duration, fetch func() (interface{}, error)) error {
	if err := ensureValidCacheKey(key); err != nil {
		return err
	}

	cached, fresh, err := c.get(key, result)
	if err != nil {
		// Log and ensure we will not try to use the result, but let everything continue because we can still try to fetch fresh data.
		logGetFromCacheError(key, err, cached, fresh)
		cached = false
		fresh = false
	} else if fresh {
		return nil
	}

	value, fetchErr := fetch()
	if fetchErr != nil {
		if cached {
			// We have an error, but we want to behave as if we do not. Just log it.
			logStaleCacheUsed(key, fetchErr)
			return nil
		}
		return errors.Wrapf(fetchErr, "Failed to fetch data and no stale version found")
	}

	c.Set(key, value, duration)
	return reflext.SetPointer(result, value)
}

// Get tries to get a fresh cached version of the data.
func (c *staleFallbackCache) Get(key string, result interface{}) (bool, error) {
	_, fresh, err := c.get(key, result)
	return fresh, err
}

func (c *staleFallbackCache) GetStale(key string, result interface{}) (bool, error) {
	cached, _, err := c.get(key, result)
	return cached, err
}

func (c *staleFallbackCache) Set(key string, value interface{}, duration time.Duration) error {
	if err := ensureValidCacheKey(key); err != nil {
		return err
	}

	cachedData, err := newCachedValue(value, duration)
	if err != nil {
		return errors.Wrapf(err, "Unable to set cache value")
	}

	return c.cache.Set(key, cachedData, c.staleTTL)
}

func (c *staleFallbackCache) get(key string, result interface{}) (cached bool, fresh bool, err error) {
	var cachedData cachedValue
	cached, err = c.cache.Get(key, &cachedData)
	if err != nil || !cached {
		return false, false, err
	}

	return true, cachedData.TTL() > 0, json.Unmarshal(cachedData.Value, result)
}

func logStaleCacheUsed(key string, err error) {
	logger(staleCacheLogCategory, "used_stale_cache", key).
		WithError(err).
		Warn("Stale cache used")
}

func logGetFromCacheError(key string, err error, cached, fresh bool) {
	logger(staleCacheLogCategory, "get_from_cache_error", key).
		WithField("cached", cached).
		WithField("fresh", fresh).
		WithError(err).
		Error("Failed to get data from cache")
}
