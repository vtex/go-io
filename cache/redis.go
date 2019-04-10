package cache

import (
	"github.com/vtex/go-io/redis"
)

var redisInstance redis.Cache

func Redis() redis.Cache {
	if redisInstance == nil {
		panic("Redis connection not initialized")
	}

	return redisInstance
}

func InitRedis(config redis.RedisConfig) {
	redisInstance = redis.New(config)
}
