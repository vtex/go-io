package cache

import (
	"errors"
	"math/rand"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	. "github.com/vtex/go-io/cache/testUtils"
)

func TestHybridCache(t *testing.T) {
	local := NewFakeCache()
	remote := NewFakeCache()
	subject := Hybrid(local, remote)

	duration := 5 * time.Minute
	expectedErr := errors.New("I am expected")

	Convey("Get", t, func() {
		key := "test_hybrid_cache_get"

		local.Reset()
		remote.Reset()

		value := rand.Intn(100)

		Convey("It should return data from local cache", func() {
			subject.Set(key, value, duration)

			GetCacheHit(subject.Get, key, value)
			So(local.GetMustHaveBeenCalledWith(key, 1), ShouldBeNil)
			So(remote.GetMustNotHaveBeenCalledWith(key), ShouldBeNil)
		})

		Convey("It should not look for data in remote cache if data is in local cache", func() {
			subject.Set(key, value, duration)

			GetCacheHit(subject.Get, key, value)

			So(remote.GetMustNotHaveBeenCalledWith(key), ShouldBeNil)
		})

		Convey("It should return data from remote cache", func() {
			subject.Set(key, value, duration)
			local.DeleteKey(key)

			GetCacheHit(subject.Get, key, value)
		})

		Convey("It should copy data from remote to local cache", func() {
			subject.Set(key, value, duration)
			local.DeleteKey(key)

			GetCacheHit(subject.Get, key, value)

			So(local.SetMustHaveBeenCalledWith(key, Any, Any), ShouldBeNil)
			So(remote.GetMustHaveBeenCalledWith(key, 1), ShouldBeNil)

			GetCacheHit(subject.Get, key, value)

			So(remote.GetMustNotHaveBeenCalledWith(key), ShouldBeNil)
		})

		Convey("It should return false when both caches miss", func() {
			GetCacheMiss(subject.Get, key)
			So(local.GetMustHaveBeenCalledWith(key, 1), ShouldBeNil)
			So(remote.GetMustHaveBeenCalledWith(key, 1), ShouldBeNil)
		})

		Convey("It should return data from remote cache if local cache fails", func() {
			local.FailGetFor(key, expectedErr)

			subject.Set(key, value, duration)
			GetCacheHit(subject.Get, key, value)
		})

		Convey("It should return an error if local cache fails and remote misses", func() {
			local.FailGetFor(key, expectedErr)
			GetCacheError(subject.Get, key)
		})

		Convey("It should return an error if local misses and remote fails", func() {
			remote.FailGetFor(key, expectedErr)

			GetCacheError(subject.Get, key)
		})

		Convey("It should return data from remote if local returns corrupt data", func() {
			subject.Set(key, value, duration)

			// Overwrite local cache with corrupt data.
			local.Set(key, "Corrupt data", duration)

			GetCacheHit(subject.Get, key, value)

			So(remote.GetMustHaveBeenCalledWith(key, 1), ShouldBeNil)
		})

		Convey("It should return an error if local is corrupted and remote is a miss", func() {
			local.Set(key, "Corrupt data", duration)

			GetCacheError(subject.Get, key)

			So(remote.GetMustHaveBeenCalledWith(key, 1), ShouldBeNil)
		})

		Convey("It should return an error if local is a miss and remote is corrupted", func() {
			remote.Set(key, "Corrupt data", duration)

			GetCacheError(subject.Get, key)
		})
	})

	Convey("Set", t, func() {
		key := "test_hybrid_cache_set"
		value := 16

		local.Reset()
		remote.Reset()

		Convey("It should set local cache", func() {
			So(subject.Set(key, value, duration), ShouldBeNil)

			So(local.SetMustHaveBeenCalledWith(key, Any, duration), ShouldBeNil)
		})

		Convey("It should set remote cache", func() {
			So(subject.Set(key, value, duration), ShouldBeNil)

			So(remote.SetMustHaveBeenCalledWith(key, Any, duration), ShouldBeNil)
		})

		Convey("It should get the correct data from local cache after setting it", func() {
			So(subject.Set(key, value, duration), ShouldBeNil)

			GetCacheHit(subject.Get, key, value)
		})

		Convey("It should get the correct data from remote cache after setting it", func() {
			So(subject.Set(key, value, duration), ShouldBeNil)

			local.DeleteKey(key)

			GetCacheHit(subject.Get, key, value)
		})

		Convey("It should return an error if it cannot save data in local cache", func() {
			local.FailSetFor(key, expectedErr)
			So(subject.Set(key, value, duration), ShouldNotBeNil)
		})

		Convey("It should return an error if it cannot save data in remote cache", func() {
			remote.FailSetFor(key, expectedErr)
			So(subject.Set(key, value, duration), ShouldNotBeNil)
		})
	})

	Convey("GetOrSet", t, func() {
		key := "test_hybrid_cache_get"

		local.Reset()
		remote.Reset()

		value := rand.Intn(100)

		Convey("It should return cached data", func() {
			subject.Set(key, value, duration)

			GetOrSetCached(subject.GetOrSet, key, duration, value)
		})

		Convey("It should fetch the data if cache misses", func() {
			GetOrSetFetch(subject.GetOrSet, key, duration, value)
		})

		Convey("It should return the error from fetch", func() {
			GetOrSetError(subject.GetOrSet, key, duration, expectedErr)
		})

		Convey("It should cache the data that has been fetched", func() {
			GetOrSetFetch(subject.GetOrSet, key, duration, value)

			GetOrSetCached(subject.GetOrSet, key, duration, value)
		})

		Convey("It should fetch the data if local cache is corrupted", func() {
			local.Set(key, "Corrupt data", duration)

			GetOrSetFetch(subject.GetOrSet, key, duration, value)
		})

		Convey("It should fetch the data if remote cache is corrupted", func() {
			remote.Set(key, "Corrupt data", duration)

			GetOrSetFetch(subject.GetOrSet, key, duration, value)
		})

		Convey("If multiple errors happen, it should return the error from fetch", func() {
			local.FailGetFor(key, expectedErr)
			remote.FailGetFor(key, expectedErr)

			fetchErr := errors.New("I should be returned")
			GetOrSetError(subject.GetOrSet, key, duration, fetchErr)
		})
	})
}
