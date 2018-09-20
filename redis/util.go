package redis

import (
	"github.com/Sirupsen/logrus"
)

func logError(err error, code, namespace, key, msg string) {
	logger := logrus.WithError(err).
		WithFields(logrus.Fields{
			"code":         code,
			"category":     "redis_cache",
			"keyNamespace": namespace,
		})
	if key != "" {
		logger = logger.WithField("key", key)
	}
	logger.Error(msg)
}
