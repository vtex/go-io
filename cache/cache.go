package cache

import (
	"time"
)

type Cache interface {
	Get(key string, result interface{}) (hit bool, err error)
	Set(key string, value interface{}, duration time.Duration) error
	GetOrSet(key string, result interface{}, duration time.Duration, fetch func() (interface{}, error)) error
}

type Stale interface {
	Cache
	GetStale(key string, result interface{}) (hit bool, err error)
}
