package reflext

import (
	"reflect"

	"github.com/pkg/errors"
)

func SetPointer(dstPtr, srcValue interface{}) (err error) {
	defer func() {
		if r := recover(); r != nil {
			switch rerr := r.(type) {
			case error:
				err = rerr
			case string:
				err = errors.New(rerr)
			default:
				err = errors.Errorf("Panic in reflective code: %s", rerr)
			}
		}
	}()

	dstPtrRv := reflect.ValueOf(dstPtr)
	if dstPtrRv.Kind() != reflect.Ptr {
		return errors.New("Value and result must have the same type")
	}

	valueRv := reflect.ValueOf(srcValue)
	if dstPtrRv.Elem().Type() != valueRv.Type() {
		return errors.New("Value and pointer incompatible types")
	}
	dstPtrRv.Elem().Set(valueRv)
	return nil
}
