package cache

import (
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// ensureValidCacheKey is a utility function that centralizes checking of keys that is repeated in many functions.
func ensureValidCacheKey(key string) error {
	if key == "" {
		return errors.Errorf("Cache key must not be empty")
	}
	return nil
}

// logger returns a logger with some default fields filled in with default keys.
func logger(category, code, key string) *logrus.Entry {
	return logrus.WithFields(logrus.Fields{
		"category": category,
		"key":      key,
		"code":     code,
	})
}
