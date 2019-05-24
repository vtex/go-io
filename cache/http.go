package cache

import (
	"time"

	"github.com/die-net/lrucache"
	"github.com/gregjones/httpcache"
)

const (
	maxCacheSize = 512 * 1024 * 1024  // 512MB
	maxCacheAge  = 3 * 24 * time.Hour // 3 days
)

func HTTP() httpcache.Cache {
	return lrucache.New(maxCacheSize, (int64)(maxCacheAge/time.Second))
}
