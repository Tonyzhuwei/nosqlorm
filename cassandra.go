package nosqlorm

import (
	"errors"
	"fmt"
	"github.com/gocql/gocql"
	"reflect"
	"strings"
	"sync"
	"time"
	"unsafe"
)

const CQL_TAG = "cql"
const JSON_TAG = "json"

var modelCache sync.Map

type tableSchema struct {
	fields   []string
	fieldMap map[string]tableField
}

type tableField struct {
	filedName       string
	isPartitionKey  bool
	isClusteringKey bool
	isStatic        bool
	dataType        reflect.Kind
	isPointer       bool
	offSet          uintptr
}

type cqlOrm[T interface{}] struct {
	sess *gocql.Session
}

// NewCqlOrm Create a new object to access specific Cassandra table
func NewCqlOrm[T interface{}](session *gocql.Session) *cqlOrm[T] {
	// Cache table schema to memory
	var t T
	typ := reflect.TypeOf(t)
	typName := typ.String()

	if typ.Kind() != reflect.Struct {
		panic("Must be struct")
	}

	if _, existing := modelCache.Load(typName); !existing {
		schema := tableSchema{
			fields:   make([]string, 0),
			fieldMap: make(map[string]tableField),
		}
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			tag := field.Tag
			filedName := getFieldName(tag)
			fieldType := field.Type.Kind()
			isPointer := false
			if field.Type.Kind() == reflect.Ptr {
				fieldType = field.Type.Elem().Kind()
				isPointer = true
			}
			schema.fields = append(schema.fields, filedName)
			schema.fieldMap[filedName] = tableField{
				filedName:       filedName,
				isPartitionKey:  isPartitionKey(tag),
				isClusteringKey: isClusterKey(tag),
				isStatic:        isStaticFiled(tag),
				dataType:        fieldType,
				isPointer:       isPointer,
				offSet:          field.Offset,
			}
		}
		modelCache.Store(typName, schema)
	}

	orm := &cqlOrm[T]{sess: session}
	return orm
}

// Auto create or update table for Cassandra
func CreateCassandraTables(sess *gocql.Session, tables ...interface{}) error {
	for _, table := range tables {
		typ := reflect.TypeOf(table)
		tableName := strings.ToLower(typ.Name())
		fields := make([]string, 0)
		pkKeys := make([]string, 0)
		ckKeys := make([]string, 0)
		for i := 0; i < typ.NumField(); i++ {
			tag := typ.Field(i).Tag
			filedName := getFieldName(tag)
			filedDBType, _ := getFieldDBType(typ.Field(i).Type.String(), tag)
			isStatic := ""
			if isStaticFiled(tag) {
				isStatic = " static"
			}
			fields = append(fields, fmt.Sprintf("%s %s%s", filedName, filedDBType, isStatic))
			if isPartitionKey(tag) {
				pkKeys = append(pkKeys, filedName)
			}
			if isClusterKey(tag) {
				ckKeys = append(ckKeys, filedName)
			}
		}
		fieldSql := strings.Join(fields, ", ")
		pkSql := strings.Join(pkKeys, ", ")
		if len(pkKeys) > 1 {
			pkSql = "(" + pkSql + ")"
		} else if len(pkKeys) == 0 {
			panic(tableName + " must have at least one Partition Key")
		}
		ckSql := strings.Join(ckKeys, ", ")
		if len(ckKeys) > 0 {
			ckSql = ", " + ckSql
		}
		sql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s, PRIMARY KEY (%s%s));", tableName, fieldSql, pkSql, ckSql)
		println(sql)
		err := sess.Query(sql).Exec()
		if err != nil {
			return errors.New(fmt.Sprintf("Create Cassandra Tables %s failed: %s\n", tableName, err.Error()))
		}
		fmt.Printf("Create Cassandra table %s success\n", tableName)
	}
	return nil
}

func (ctx *cqlOrm[T]) Insert(obj T) error {
	val := reflect.ValueOf(obj)
	typ := val.Type()
	tableName := strings.ToLower(typ.Name())

	schema, ok := modelCache.Load(typ.String())
	if !ok {
		return errors.New(fmt.Sprintf("Table %s not found", tableName))
	}
	tableFields := schema.(tableSchema).fields

	insertFields := make([]string, 0)
	fieldPlaceHolders := make([]string, 0)
	sqlValues := make([]interface{}, 0)
	for i := range tableFields {
		fieldVal := convertToNormalValue(val.Field(i))
		if fieldVal == nil {
			continue
		}
		insertFields = append(insertFields, tableFields[i])
		fieldPlaceHolders = append(fieldPlaceHolders, "?")
		sqlValues = append(sqlValues, fieldVal)
	}
	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);", tableName, strings.Join(insertFields, ","), strings.Join(fieldPlaceHolders, ","))

	return ctx.sess.Query(sql, sqlValues...).Exec()
}

func (ctx *cqlOrm[T]) Select(obj T) ([]T, error) {
	val := reflect.ValueOf(obj)
	typ := val.Type()
	tableName := strings.ToLower(typ.Name())

	schema, ok := modelCache.Load(typ.String())
	tableFields := schema.(tableSchema).fields
	if !ok {
		return []T{}, errors.New(fmt.Sprintf("Table %s not found", tableName))
	}

	selectFields := make([]string, 0)
	whereClause := make([]string, 0)
	whereValues := make([]interface{}, 0)

	for i, fieldName := range tableFields {
		selectFields = append(selectFields, fieldName)
		if schema.(tableSchema).fieldMap[fieldName].isClusteringKey || schema.(tableSchema).fieldMap[fieldName].isPartitionKey {
			fieldVal := convertToNormalValue(val.Field(i))
			if fieldVal == nil {
				continue
			}
			whereClause = append(whereClause, fmt.Sprintf("%s=?", fieldName))
			whereValues = append(whereValues, fieldVal)
		}
	}

	sql := fmt.Sprintf("SELECT %s FROM %s WHERE %s;", strings.Join(selectFields, ", "), tableName, strings.Join(whereClause, " AND "))
	selectResult := make([]T, 0)
	iter := ctx.sess.Query(sql, whereValues...).Iter()
	defer func() {
		err := iter.Close()
		if err != nil {
			fmt.Printf("Select %s failed: %s\n", tableName, err.Error())
		}
	}()
	for {
		var tableObj T
		if !iter.Scan(getPointersOfStructElements(unsafe.Pointer(&tableObj), selectFields, schema.(tableSchema).fieldMap)...) {
			break
		}
		selectResult = append(selectResult, tableObj)
	}

	return selectResult, nil
}

func (ctx *cqlOrm[T]) Update(obj T) error {
	val := reflect.ValueOf(obj)
	typ := val.Type()
	tableName := strings.ToLower(typ.Name())

	schema, ok := modelCache.Load(typ.String())
	if !ok {
		return errors.New(fmt.Sprintf("Table %s not found", tableName))
	}
	tableFields := schema.(tableSchema).fields

	fields := make([]string, 0)
	whereClause := make([]string, 0)
	sqlValues := make([]interface{}, 0)
	whereValues := make([]interface{}, 0)
	for i, filedName := range tableFields {
		fieldVal := convertToNormalValue(val.Field(i))
		if fieldVal == nil {
			continue
		}
		if schema.(tableSchema).fieldMap[filedName].isPartitionKey || schema.(tableSchema).fieldMap[filedName].isClusteringKey {
			whereClause = append(whereClause, fmt.Sprintf("%s=?", filedName))
			whereValues = append(whereValues, fieldVal)
		} else {
			fields = append(fields, filedName+"=?")
			sqlValues = append(sqlValues, fieldVal)
		}
	}

	sql := fmt.Sprintf("UPDATE %s SET %s WHERE %s;", tableName, strings.Join(fields, ","), strings.Join(whereClause, " AND "))
	//fmt.Println(sql)
	//fmt.Println(sqlValues)

	sqlParams := sqlValues
	sqlParams = append(sqlParams, whereValues...)
	return ctx.sess.Query(sql, sqlParams...).Exec()
}

func (ctx *cqlOrm[T]) Delete(obj T) error {
	val := reflect.ValueOf(obj)
	typ := val.Type()
	tableName := strings.ToLower(typ.Name())

	schema, ok := modelCache.Load(typ.String())
	if !ok {
		return errors.New(fmt.Sprintf("Table %s not found", tableName))
	}

	whereClause := make([]string, 0)
	whereValues := make([]interface{}, 0)
	for i, fieldName := range schema.(tableSchema).fields {
		if schema.(tableSchema).fieldMap[fieldName].isPartitionKey || schema.(tableSchema).fieldMap[fieldName].isClusteringKey {
			fieldVal := convertToNormalValue(val.Field(i))
			if fieldVal == nil {
				continue
			}
			whereClause = append(whereClause, fmt.Sprintf("%s=?", fieldName))
			whereValues = append(whereValues, fieldVal)
		}
	}

	sql := fmt.Sprintf("DELETE FROM %s WHERE %s;", tableName, strings.Join(whereClause, " AND "))
	//fmt.Println(sql)
	//fmt.Println(whereValues)

	return ctx.sess.Query(sql, whereValues...).Exec()
}

// Mapping golang type to CS data type
func getFieldDBType(typeName string, tag reflect.StructTag) (string, error) {
	if typeName[0:1] == "*" {
		typeName = typeName[1:]
	}
	if typeName[0:2] == "[]" {
		typeName = typeName[2:]
		listDateTYpe, _ := getFieldDBType(typeName, tag)
		return fmt.Sprintf("list<%s>", listDateTYpe), nil
	}
	//if typeName[0:3] == "map" {
	//	re := regexp.MustCompile(`^map\[(.+)\](.+)$`)
	//	matchedStr := re.FindStringSubmatch(typeName)
	//	if len(matchedStr) != 3 {
	//		return "", errors.New(fmt.Sprintf("%s is not a map type", typeName))
	//	}
	//	keyType, _ := getFieldDBType(matchedStr[1], tag)
	//	valType, _ := getFieldDBType(matchedStr[2], tag)
	//	return fmt.Sprintf("map<%s,%s>", keyType, valType), nil
	//}

	switch typeName {
	case "bool":
		return "boolean", nil
	case "string":
		return "text", nil
	case "int8":
		return "tinyint", nil
	case "int16":
		return "smallint", nil
	case "int32":
		return "int", nil
	case "int":
		return "bigint", nil
	case "int64":
		return "bigint", nil
	case "float32":
		return "float", nil
	case "float64":
		return "double", nil
	case "time.Time":
		if isDateFiled(tag) {
			return "date", nil
		}
		return "timestamp", nil
	default:
		return "", errors.New("Invalid type: " + typeName)
	}
}

func isPartitionKey(tag reflect.StructTag) bool {
	return strings.Contains(tag.Get(CQL_TAG), "pk")
}

func isClusterKey(tag reflect.StructTag) bool {
	return strings.Contains(tag.Get(CQL_TAG), "ck")
}

func isStaticFiled(tag reflect.StructTag) bool {
	return strings.Contains(tag.Get(CQL_TAG), "static")
}

func isDateFiled(tag reflect.StructTag) bool {
	return strings.Contains(tag.Get(CQL_TAG), "date")
}

// Get pointers of struct elements for data scanning usage.
func getPointersOfStructElements(basePoint unsafe.Pointer, selectFields []string, fieldsMap map[string]tableField) []interface{} {
	fieldsPtr := make([]interface{}, 0)
	for _, val := range selectFields {
		field, _ := fieldsMap[val]
		switch field.dataType {
		case reflect.Bool:
			appendPtr[bool](&fieldsPtr, basePoint, field)
		case reflect.Int:
			appendPtr[int](&fieldsPtr, basePoint, field)
		case reflect.Int8:
			appendPtr[int8](&fieldsPtr, basePoint, field)
		case reflect.Int16:
			appendPtr[int16](&fieldsPtr, basePoint, field)
		case reflect.Int32:
			appendPtr[int32](&fieldsPtr, basePoint, field)
		case reflect.Int64:
			appendPtr[int64](&fieldsPtr, basePoint, field)
		case reflect.Uint:
			appendPtr[uint](&fieldsPtr, basePoint, field)
		case reflect.Uint8:
			appendPtr[uint8](&fieldsPtr, basePoint, field)
		case reflect.Uint16:
			appendPtr[uint16](&fieldsPtr, basePoint, field)
		case reflect.Uint32:
			appendPtr[uint32](&fieldsPtr, basePoint, field)
		case reflect.Uint64:
			appendPtr[uint64](&fieldsPtr, basePoint, field)
		case reflect.Uintptr:
			appendPtr[uintptr](&fieldsPtr, basePoint, field)
		case reflect.Float32:
			appendPtr[float32](&fieldsPtr, basePoint, field)
		case reflect.Float64:
			appendPtr[float64](&fieldsPtr, basePoint, field)
		case reflect.Complex64:
			appendPtr[complex64](&fieldsPtr, basePoint, field)
		case reflect.Complex128:
			appendPtr[complex128](&fieldsPtr, basePoint, field)
		case reflect.String:
			appendPtr[string](&fieldsPtr, basePoint, field)
		case reflect.Slice:
			fieldsPtr = append(fieldsPtr, (*[]int)(unsafe.Add(basePoint, field.offSet))) // TODO: to be support more
		case reflect.Struct:
			appendPtr[time.Time](&fieldsPtr, basePoint, field) // TODO: to be support more
		default:
			fieldsPtr = append(fieldsPtr, nil)
		}
	}
	return fieldsPtr
}

func appendPtr[T any](fieldsPtr *[]interface{}, basePoint unsafe.Pointer, field tableField) {
	if field.isPointer {
		*fieldsPtr = append(*fieldsPtr, (**T)(unsafe.Add(basePoint, field.offSet)))
		return
	}
	*fieldsPtr = append(*fieldsPtr, (*T)(unsafe.Add(basePoint, field.offSet)))
}
