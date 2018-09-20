package redis

import (
	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
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

func remoteKey(ns, key string) (string, error) {
	if key == "" {
		return "", errors.Errorf("Cache key must not be empty (namespace: %s)", ns)
	}
	return ns + ":" + key, nil
}

func remoteKeys(ns string, keys []string) ([]string, error) {
	var err error
	remotes := make([]string, len(keys))
	for i, key := range keys {
		remotes[i], err = remoteKey(ns, key)
		if err != nil {
			return nil, err
		}
	}
	return remotes, nil
}
