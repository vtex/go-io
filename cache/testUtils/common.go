package testUtils

import (
	"time"

	"github.com/pkg/errors"
	. "github.com/smartystreets/goconvey/convey"
)

// Fetch simplifies the code when writing fetch functions to be used by GetOrSet.
func Fetch(value interface{}, err error) func() (interface{}, error) {
	return func() (interface{}, error) {
		return value, err
	}
}

// FetchPanic returns a function that should not be called to be passed to GetOrSet.
func FetchPanic() (interface{}, error) {
	// Use "So" to make error clearer.
	So(func() { panic("Fetch function should not have been called") }, ShouldNotPanic)
	return nil, nil
}

// The functions below receive the function instead of a cache instance to avoid circular dependencies and to allow
// reuse by any methods that have the same signature.

func GetOrSetCached(getOrSet func(string, interface{}, time.Duration, func() (interface{}, error)) error, key string, duration time.Duration, expectedValue int) {
	var data int
	So(getOrSet(key, &data, duration, FetchPanic), ShouldBeNil)
	So(data, ShouldEqual, expectedValue)
}

func GetOrSetFetch(getOrSet func(string, interface{}, time.Duration, func() (interface{}, error)) error, key string, duration time.Duration, expectedValue int) {
	var data int
	So(getOrSet(key, &data, duration, Fetch(expectedValue, nil)), ShouldBeNil)
	So(data, ShouldEqual, expectedValue)
}

func GetOrSetError(getOrSet func(string, interface{}, time.Duration, func() (interface{}, error)) error, key string, duration time.Duration, expectedErr error) {
	var data int
	err := getOrSet(key, &data, duration, Fetch(nil, expectedErr))
	So(err, ShouldNotBeNil)
	So(errors.Cause(err), ShouldEqual, expectedErr)
}

func GetCacheHit(get func(key string, result interface{}) (bool, error), key string, expectedValue int) {
	var data int
	cached, err := get(key, &data)
	So(err, ShouldBeNil)
	So(cached, ShouldBeTrue)
	So(data, ShouldEqual, expectedValue)
}

func GetCacheMiss(get func(key string, result interface{}) (bool, error), key string) {
	var data int
	cached, err := get(key, &data)
	So(err, ShouldBeNil)
	So(cached, ShouldBeFalse)
}

func GetCacheError(get func(key string, result interface{}) (bool, error), key string) {
	var data int
	_, err := get(key, &data)
	So(err, ShouldNotBeNil)
}
