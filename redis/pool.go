package redis

import (
	"time"

	"github.com/gomodule/redigo/redis"
)

const (
	dialTimeout       = 1 * time.Second
	writeTimeout      = 200 * time.Millisecond
	readTimeout       = 200 * time.Millisecond
	idlePingThreshold = 30 * time.Second
	idleConnTimeout   = 3 * time.Minute
)

type poolOptions struct {
	MaxIdle, MaxActive int
	SetReadTimeout     bool
}

func newRedisPool(endpoint string, opts poolOptions) *redis.Pool {
	dialOpts := []redis.DialOption{
		redis.DialConnectTimeout(dialTimeout),
		redis.DialWriteTimeout(writeTimeout),
	}
	if opts.SetReadTimeout {
		dialOpts = append(dialOpts, redis.DialReadTimeout(readTimeout))
	}
	return &redis.Pool{
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", endpoint, dialOpts...)
		},
		TestOnBorrow: func(conn redis.Conn, idleSince time.Time) error {
			if time.Since(idleSince) < idlePingThreshold {
				return nil
			}
			_, err := conn.Do("PING")
			return err
		},
		MaxIdle:     opts.MaxIdle,
		MaxActive:   opts.MaxActive,
		Wait:        true,
		IdleTimeout: idleConnTimeout,
	}
}
