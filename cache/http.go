package cache

import (
	"time"

	"github.com/die-net/lrucache"
	"github.com/gregjones/httpcache"
)

func HTTP(maxCacheSizeMiB int, maxCacheAge time.Duration) httpcache.Cache {
	return lrucache.New(maxCacheSize*1024*1024, (int64)(maxCacheAge/time.Second))
}
