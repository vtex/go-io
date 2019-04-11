package cache

import (
	"encoding/json"
	"time"
)

// cachedValue is a struct that allows us to save some data together with its expiration time.
// This is useful if we want to keep the data for some time after its expired or if we need to share the expiration
// with other instances that access the same cache.
type cachedValue struct {
	FreshUntil time.Time
	Value      json.RawMessage
}

func newCachedValue(value interface{}, duration time.Duration) (cachedValue, error) {
	bytes, err := json.Marshal(value)
	if err != nil {
		return cachedValue{}, err
	}

	return cachedValue{
		FreshUntil: time.Now().Add(duration),
		Value:      json.RawMessage(bytes),
	}, nil
}

func (c cachedValue) TTL() time.Duration {
	return c.FreshUntil.Sub(time.Now())
}
