package cache

import (
	"time"

	"github.com/die-net/lrucache"
	"github.com/gregjones/httpcache"
)

func HTTP(maxCacheSizeMiB int64, maxCacheAge time.Duration) httpcache.Cache {
	cacheSizeBytes := maxCacheSizeMiB * 1024 * 1024
	maxCacheAgeSec := (int64)(maxCacheAge / time.Second)
	return lrucache.New(cacheSizeBytes, maxCacheAgeSec)
}
