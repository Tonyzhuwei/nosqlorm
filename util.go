package nosqlorm

import (
	"reflect"
	"strings"
	"time"
)

// sql formating
// Convert from reflect.Value to specific normal type(E.g: Int, string and etc)
func convertToNormalValue(value reflect.Value) interface{} {
	switch value.Type().Kind() {
	case reflect.Bool:
		return value.Interface().(bool)
	case reflect.Int:
		return value.Interface().(int)
	case reflect.Int8:
		return value.Interface().(int8)
	case reflect.Int16:
		return value.Interface().(int16)
	case reflect.Int32:
		return value.Interface().(int32)
	case reflect.Int64:
		return value.Interface().(int64)
	case reflect.Uint:
		return value.Interface().(uint)
	case reflect.Uint8:
		return value.Interface().(uint8)
	case reflect.Uint16:
		return value.Interface().(uint16)
	case reflect.Uint32:
		return value.Interface().(uint32)
	case reflect.Uint64:
		return value.Interface().(uint64)
	case reflect.Uintptr:
		return value.Interface().(uintptr)
	case reflect.Float32:
		return value.Interface().(float32)
	case reflect.Float64:
		return value.Interface().(float64)
	case reflect.Complex64:
		return value.Interface().(complex64)
	case reflect.Complex128:
		return value.Interface().(complex128)
	case reflect.String:
		return value.Interface().(string)
	default:
		if value.Type().Kind() == reflect.Struct && value.Type().String() == "time.Time" {
			return value.Interface().(time.Time)
		}
	}
	return nil
}

// Fetch field name based on the tag
func getFiledName(tag reflect.StructTag) string {
	return strings.Split(tag.Get(JSON_TAG), ",")[0]
}
