package nosqlorm

import (
	"reflect"
	"strings"
	"time"
)

// sql formating
// Convert from reflect.Value to specific type(E.g: Int, string and etc)
func convertToValue(value reflect.Value) interface{} {
	typ := value.Type().Name()
	if typ == "string" && value.String() != "" {
		return value.String()
	} else if value.CanInt() && value.Int() != 0 {
		return value.Int()
	} else if typ == "time.Time" {
		return value.Interface().(time.Time)
	}
	return nil
}

// Fetch field name based on the tag
func getFiledName(tag reflect.StructTag) string {
	return strings.Split(tag.Get(JSON_TAG), ",")[0]
}
