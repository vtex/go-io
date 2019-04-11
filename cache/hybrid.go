package cache

import (
	"encoding/json"
	"time"

	"github.com/pkg/errors"
	"github.com/vtex/go-io/util"
)

const (
	hybridCacheLogCategory = "hybrid_cache"
)

func Hybrid(local, remote Cache) Cache {
	return &hybridCache{local: local, remote: remote}
}

type hybridCache struct {
	local  Cache
	remote Cache
}

func (c *hybridCache) Get(key string, result interface{}) (bool, error) {
	var jsonValue json.RawMessage
	cached, localErr := c.local.Get(key, &jsonValue)
	if localErr != nil {
		// Log, but fall back to remote cache to try to avoid disrupting the request.
		logGetLocalDataError(key, false, localErr)
	} else if cached {
		localErr = json.Unmarshal(jsonValue, result)
		if localErr == nil {
			return true, nil
		}
		logGetLocalDataError(key, true, localErr)
	}

	var remoteData cachedValue
	cached, err := c.remote.Get(key, &remoteData)
	if err != nil {
		return false, errors.Wrapf(err, "Unable to fetch data from remote cache")
	}

	if !cached {
		return false, localErr
	}

	err = json.Unmarshal(remoteData.Value, result)
	if err != nil {
		return false, errors.Wrapf(err, "Unable to save retrieved data in result variable")
	}

	// This if accounts for possible clock differences, ensuring we never write to local cache with a negative duration.
	if ttl := remoteData.TTL(); ttl > 0 {
		c.local.Set(key, remoteData.Value, ttl)
	}
	return true, nil
}

func (c *hybridCache) Set(key string, value interface{}, duration time.Duration) error {
	if err := ensureValidCacheKey(key); err != nil {
		return err
	}

	remoteData, err := newCachedValue(value, duration)
	if err != nil {
		return errors.Wrapf(err, "Failed to save data into cache")
	}

	err = c.local.Set(key, remoteData.Value, duration)
	if err != nil {
		return errors.Wrapf(err, "Failed to save data into local cache")
	}

	err = c.remote.Set(key, remoteData, duration)
	if err != nil {
		return errors.Wrapf(err, "Failed to save data into remote cache")
	}

	return nil
}

func (c *hybridCache) GetOrSet(key string, result interface{}, duration time.Duration, fetch func() (interface{}, error)) error {
	if err := ensureValidCacheKey(key); err != nil {
		return err
	}

	cached, err := c.Get(key, result)
	if err != nil {
		// We log the error, but still try to get fresh data to avoid disrupting a workflow that might still work.
		logGetRemoteDataError(key, err)
	}
	if cached {
		return nil
	}

	value, err := fetch()
	if err != nil {
		return err
	}

	c.Set(key, value, duration)
	return util.SetPointer(result, value)
}

func logGetLocalDataError(key string, cached bool, err error) {
	logger(hybridCacheLogCategory, "get_local_error", key).
		WithField("isHit", cached).
		WithError(err).
		Error("Failed to get data from local cache")
}

func logGetRemoteDataError(key string, err error) {
	logger(hybridCacheLogCategory, "get_remote_error", key).
		WithError(err).
		Error("Failed to get data from remote cache")
}
