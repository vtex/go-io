package cache

import (
	"math/rand"
	"testing"
	"time"

	"github.com/pkg/errors"
	. "github.com/smartystreets/goconvey/convey"
	. "github.com/vtex/go-io/cache/testUtils"
)

func TestStaleFallbackCache(t *testing.T) {
	store := NewFakeCache()
	staleTTL := 20 * time.Minute
	subject := WithStaleFallback(store, staleTTL)

	duration := 5 * time.Second
	expectedErr := errors.New("I am expected")

	Convey("Get", t, func() {
		key := "stale_fallback_get"

		store.Reset()

		value := rand.Intn(100)

		Convey("It should return valid data from cache", func() {
			subject.Set(key, value, duration)

			GetCacheHit(subject.Get, key, value)
		})

		Convey("It should return an error if storage fails", func() {
			store.FailGetFor(key, expectedErr)

			GetCacheError(subject.Get, key)
		})

		Convey("It should not return stale data", func() {
			subject.Set(key, value, 0)

			GetCacheMiss(subject.Get, key)
		})

		Convey("It should miss if data is not preset", func() {
			GetCacheMiss(subject.Get, key)
		})
	})

	Convey("GetStale", t, func() {
		key := "stale_fallback_get_stale"

		store.Reset()

		value := rand.Intn(100)

		Convey("It should return valid data from cache", func() {
			subject.Set(key, value, duration)

			GetCacheHit(subject.GetStale, key, value)
		})

		Convey("It should return an error if storage fails", func() {
			store.FailGetFor(key, expectedErr)

			GetCacheError(subject.GetStale, key)
		})

		Convey("It should return stale data", func() {
			subject.Set(key, value, 0)

			GetCacheHit(subject.GetStale, key, value)
		})

		Convey("It should miss if data is not present", func() {
			GetCacheMiss(subject.GetStale, key)
		})
	})

	Convey("Set", t, func() {
		key := "stale_fallback_set"

		store.Reset()

		value := rand.Intn(100)

		Convey("It should set into storage", func() {
			So(subject.Set(key, value, duration), ShouldBeNil)

			So(store.SetMustHaveBeenCalledWith(key, Any, staleTTL), ShouldBeNil)
		})

		Convey("It should retrieve cached data", func() {
			So(subject.Set(key, value, duration), ShouldBeNil)

			GetCacheHit(subject.Get, key, value)
		})

		Convey("It should forward errors from storage", func() {
			store.FailSetFor(key, expectedErr)

			So(subject.Set(key, value, duration), ShouldNotBeNil)
		})
	})

	Convey("GetOrSet", t, func() {
		key := "stale_fallback_get_or_set"

		store.Reset()

		value := rand.Intn(100)

		data := -1

		Convey("It should return data if it is valid in cache", func() {
			subject.Set(key, value, duration)

			GetOrSetCached(subject.GetOrSet, key, duration, value)
		})

		Convey("It should return fetched data if the cache is expired", func() {
			subject.Set(key, -1, 0)

			GetOrSetFetch(subject.GetOrSet, key, duration, value)
		})

		Convey("It should store the data that has been fetched", func() {
			GetOrSetFetch(subject.GetOrSet, key, duration, value)

			So(store.SetMustHaveBeenCalledWith(key, Any, Any), ShouldBeNil)
		})

		Convey("It should overwrite stale cache entry with fresh data", func() {
			subject.Set(key, -1, 0)

			GetOrSetFetch(subject.GetOrSet, key, duration, value)
			So(store.SetMustHaveBeenCalledWith(key, Any, Any), ShouldBeNil)

			GetCacheHit(subject.Get, key, value)
		})

		Convey("It should return stale data if the fetch function fails", func() {
			subject.Set(key, value, 0)

			So(subject.GetOrSet(key, &data, duration, Fetch(nil, expectedErr)), ShouldBeNil)
			So(data, ShouldEqual, value)
		})

		Convey("It should forward fetch error if cache misses", func() {
			GetOrSetError(subject.GetOrSet, key, duration, expectedErr)
		})

		Convey("It should fetch data if cache errors", func() {
			store.FailGetFor(key, expectedErr)

			GetOrSetFetch(subject.GetOrSet, key, duration, value)
		})

		Convey("It should return the *fetch* error if both cache and fetch error", func() {
			store.FailGetFor(key, expectedErr)

			fetchErr := errors.New("I am the one that should be wrapped")

			GetOrSetError(subject.GetOrSet, key, duration, fetchErr)
		})
	})
}
