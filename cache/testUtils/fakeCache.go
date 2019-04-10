package testUtils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/pkg/errors"
	"github.com/vtex/go-io/util"
)

var (
	methodGet      = "Get"
	methodSet      = "Set"
	methodGetStale = "GetStale"
	methodGetOrSet = "GetOrSet"

	Any anyMatcher = "any"
)

type FakeCache struct {
	data   map[string]*cacheEntry
	toFail map[string]error
	calls  []*methodCall
}

type cacheEntry struct {
	expiration time.Time
	value      json.RawMessage
}

type methodCall struct {
	method string
	count  int
	key    string
	args   []interface{}
}

type Matcher interface {
	Matches(value interface{}) bool
}

type anyMatcher string

func NewFakeCache() *FakeCache {
	c := &FakeCache{}
	return c.Reset()
}

func (c *FakeCache) Reset() *FakeCache {
	c.data = make(map[string]*cacheEntry)
	c.toFail = make(map[string]error)
	c.calls = make([]*methodCall, 0, 10)
	return c
}

func (c *FakeCache) Get(key string, result interface{}) (hit bool, err error) {
	c.logCall(methodGet, key)
	if err := c.shouldFail(methodGet, key); err != nil {
		return false, err
	}
	return c.get(key, result)
}

func (c *FakeCache) Set(key string, value interface{}, duration time.Duration) error {
	c.logCall(methodSet, key, value, duration)
	if err := c.shouldFail(methodSet, key); err != nil {
		return err
	}
	return c.Populate(key, value, duration)
}

func (c *FakeCache) GetOrSet(key string, result interface{}, duration time.Duration, fetch func() (interface{}, error)) error {
	c.logCall(methodGetOrSet, key, duration)
	if err := c.shouldFail(methodGetOrSet, key); err != nil {
		return err
	}

	hit, err := c.get(key, result)
	if err != nil {
		// Since this is for testing, errors are always reported.
		return errors.Wrapf(err, "Failed to get from local cache")
	}
	if hit {
		return nil
	}

	value, err := fetch()
	if err != nil {
		return errors.Wrapf(err, "Fetch failed")
	}

	err = c.Populate(key, value, duration)
	if err != nil {
		return errors.Wrapf(err, "Failed to save fetched value to cache")
	}

	err = util.SetPointer(result, value)
	if err != nil {
		return errors.Wrapf(err, "Failed to copy fetched value to result variable")
	}

	return nil
}

func (c *FakeCache) GetStale(key string, result interface{}) (hit bool, err error) {
	c.logCall(methodGetStale, key)
	if err := c.shouldFail(methodGetStale, key); err != nil {
		return false, err
	}
	return false, errors.Errorf("Not implemented")
}

func (c *FakeCache) ExpireKey(key string) bool {
	e, ok := c.data[key]
	if ok {
		e.expiration = time.Now()
	}
	return ok
}

func (c *FakeCache) DeleteKey(key string) bool {
	_, ok := c.data[key]
	if ok {
		delete(c.data, key)
	}
	return ok
}

func (c *FakeCache) get(key string, result interface{}) (hit bool, err error) {
	entry, exists := c.data[key]
	if !exists {
		return false, nil
	}

	if entry.expiration.Before(time.Now()) {
		return false, nil
	}

	err = json.Unmarshal(entry.value, result)
	if err != nil {
		return false, errors.Wrapf(err, "Failed to unmarshal cached data")
	}

	return true, nil
}

func (c *FakeCache) Populate(key string, value interface{}, duration time.Duration) error {
	bytes, err := json.Marshal(value)
	if err != nil {
		return errors.Wrapf(err, "Failed to save data to fake cache")
	}

	c.data[key] = &cacheEntry{
		expiration: time.Now().Add(duration),
		value:      json.RawMessage(bytes),
	}
	return nil
}

func (c *FakeCache) GetMustHaveBeenCalledWith(key string, times int) error {
	return c.ensureCalled(methodGet, times, key)
}

func (c *FakeCache) SetMustHaveBeenCalledWith(key string, value interface{}, duration interface{}) error {
	return c.ensureCalled(methodSet, -1, key, value, duration)
}

func (c *FakeCache) GetStaleMustHaveBeenCalledWith(key string, times int) error {
	return c.ensureCalled(methodGetStale, times, key)
}

func (c *FakeCache) GetOrSetMustHaveBeenCalledWith(key string, times int, duration interface{}) error {
	return c.ensureCalled(methodGetOrSet, times, key, duration)
}

func (c *FakeCache) GetMustNotHaveBeenCalledWith(key string) error {
	return c.ensureNotCalled(methodGet, key)
}

func (c *FakeCache) SetMustNotHaveBeenCalledWith(key string, value interface{}, duration interface{}) error {
	return c.ensureNotCalled(methodSet, key, value, duration)
}

func (c *FakeCache) GetStaleMustNotHaveBeenCalledWith(key string) error {
	return c.ensureNotCalled(methodGetStale, key)
}

func (c *FakeCache) GetOrSetMustNotHaveBeenCalledWith(key string, duration interface{}) error {
	return c.ensureNotCalled(methodGetOrSet, key, duration)
}

func (c *FakeCache) FailGetFor(key string, err error) {
	c.failMethodFor(methodGet, key, err)
}

func (c *FakeCache) FailSetFor(key string, err error) {
	c.failMethodFor(methodSet, key, err)
}

func (c *FakeCache) FailGetStaleFor(key string, err error) {
	c.failMethodFor(methodGetStale, key, err)
}

func (c *FakeCache) FailGetOrSetFor(key string, err error) {
	c.failMethodFor(methodGetOrSet, key, err)
}

func (c *FakeCache) logCall(method, key string, args ...interface{}) {
	call := c.findCallMatching(method, key, args...)
	if call != nil {
		call.count++
	}

	c.calls = append(c.calls, &methodCall{
		method: method,
		count:  1,
		key:    key,
		args:   args,
	})
}

func (c *FakeCache) ensureCalled(method string, times int, key string, args ...interface{}) error {
	call := c.findCallMatching(method, key, args...)
	if call == nil {
		return errors.Errorf("Expected %s(%s) to have been called, but it was not", method, key)
	}

	if times >= 0 && call.count != times {
		return errors.Errorf("Expected %s(%s) to have been called %d times, but it was called %d times", method, key, times, call.count)
	}
	call.count = 0

	return nil
}

func (c *FakeCache) ensureNotCalled(method, key string, args ...interface{}) error {
	call := c.findCallMatching(method, key, args...)
	if call != nil && call.count != 0 {
		return errors.Errorf("Expected %s(%s) to have not been called, but it was", method, key)
	}
	return nil
}

func (c *FakeCache) findCallMatching(method, key string, args ...interface{}) *methodCall {
	for _, call := range c.calls {
		if call.matches(method, key, args...) {
			return call
		}
	}
	return nil
}

func (c *FakeCache) failMethodFor(method, key string, err error) {
	c.toFail[failFingerprint(method, key)] = err
}

func (c *FakeCache) shouldFail(method, key string) error {
	return c.toFail[failFingerprint(method, key)]
}

func (c *methodCall) matches(method, key string, args ...interface{}) bool {
	if c.method != method || c.key != key || len(c.args) != len(args) {
		return false
	}

	for i, value := range c.args {
		if m, isMatcher := args[i].(Matcher); isMatcher {
			if !m.Matches(value) {
				return false
			}
			continue
		}

		v := reflect.ValueOf(value)
		a := reflect.ValueOf(args[i])
		if v.Type() != a.Type() {
			return false
		}

		callBytes, _ := json.Marshal(value)
		argBytes, _ := json.Marshal(args[i])

		if !bytes.Equal(callBytes, argBytes) {
			return false
		}
	}

	return true
}

func (m anyMatcher) Matches(value interface{}) bool {
	return true
}

func failFingerprint(method, key string) string {
	return fmt.Sprintf("%s::%s", method, key)
}
