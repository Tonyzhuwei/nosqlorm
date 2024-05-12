package nosqlorm

import (
	"errors"
	"fmt"
	"github.com/gocql/gocql"
	"reflect"
	"strings"
	"sync"
	"unsafe"
)

const CQL_TAG = "cql"
const JSON_TAG = "json"

var modelCache sync.Map

type cassandraTableField struct {
	isPartitionKey  bool
	isClusteringKey bool
	isStatic        bool
	index           int
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
		tableSchema := make(map[string]cassandraTableField, 0)
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			tag := field.Tag
			fieldType := field.Type.Kind()
			isPointer := false
			if field.Type.Kind() == reflect.Ptr {
				fieldType = field.Type.Elem().Kind()
				isPointer = true
			}
			tableSchema[field.Name] = cassandraTableField{
				isPartitionKey:  isPartitionKey(tag),
				isClusteringKey: isClusterKey(tag),
				isStatic:        isStaticFiled(tag),
				index:           field.Index[0],
				dataType:        fieldType,
				isPointer:       isPointer,
				offSet:          field.Offset,
			}
		}
		modelCache.Store(typName, tableSchema)
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
			filedDBType := getFieldDBType(typ.Field(i).Type, tag)
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

	tableSchema, ok := modelCache.Load(typ.String())
	if !ok {
		return errors.New(fmt.Sprintf("Table %s not found", tableName))
	}
	tableFields := tableSchema.(map[string]cassandraTableField)

	insertFields := make([]string, 0)
	fieldPlaceHolders := make([]string, 0)
	sqlValues := make([]interface{}, 0)
	for key := range tableFields {
		fieldVal := convertToNormalValue(val.FieldByName(key))
		if fieldVal == nil {
			continue
		}
		insertFields = append(insertFields, key)
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

	tableSchema, ok := modelCache.Load(typ.String())
	tableFields := tableSchema.(map[string]cassandraTableField)
	if !ok {
		return []T{}, errors.New(fmt.Sprintf("Table %s not found", tableName))
	}

	selectFields := make([]string, 0)
	whereClause := make([]string, 0)
	whereValues := make([]interface{}, 0)
	// TODO: sort keys by order

	for key, value := range tableFields {
		fieldName := key
		selectFields = append(selectFields, fieldName)
		if value.isClusteringKey || value.isPartitionKey {
			fieldVal := convertToNormalValue(val.FieldByName(fieldName))
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
		if !iter.Scan(getPointersOfStructElements(unsafe.Pointer(&tableObj), selectFields, tableFields)...) {
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

	tableSchema, ok := modelCache.Load(typ.String())
	if !ok {
		return errors.New(fmt.Sprintf("Table %s not found", tableName))
	}
	tableFields := tableSchema.(map[string]cassandraTableField)

	fields := make([]string, 0)
	whereClause := make([]string, 0)
	sqlValues := make([]interface{}, 0)
	whereValues := make([]interface{}, 0)
	for key := range tableFields {
		fieldVal := convertToNormalValue(val.FieldByName(key))
		if fieldVal == nil {
			continue
		}
		if tableFields[key].isPartitionKey || tableFields[key].isClusteringKey {
			whereClause = append(whereClause, fmt.Sprintf("%s=?", key))
			whereValues = append(whereValues, fieldVal)
		} else {
			fields = append(fields, key+"=?")
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

	tableSchema, ok := modelCache.Load(typ.String())
	if !ok {
		return errors.New(fmt.Sprintf("Table %s not found", tableName))
	}
	tableFields := tableSchema.(map[string]cassandraTableField)

	whereClause := make([]string, 0)
	whereValues := make([]interface{}, 0)
	for key := range tableFields {
		if tableFields[key].isPartitionKey || tableFields[key].isClusteringKey {
			fieldVal := convertToNormalValue(val.FieldByName(key))
			if fieldVal == nil {
				continue
			}
			whereClause = append(whereClause, fmt.Sprintf("%s=?", key))
			whereValues = append(whereValues, fieldVal)
		}
	}

	sql := fmt.Sprintf("DELETE FROM %s WHERE %s;", tableName, strings.Join(whereClause, " AND "))
	//fmt.Println(sql)
	//fmt.Println(whereValues)

	return ctx.sess.Query(sql, whereValues...).Exec()
}

// Mapping golang type to CS data type
func getFieldDBType(p reflect.Type, tag reflect.StructTag) string {
	typeName := p.String()
	if p.Kind() == reflect.Ptr {
		typeName = p.Elem().String()
	}

	switch typeName {
	case "bool":
		return "boolean"
	case "string":
		return "text"
	case "int8":
		return "tinyint"
	case "int16":
		return "smallint"
	case "int32":
		return "int"
	case "int":
		return "bigint"
	case "int64":
		return "bigint"
	case "float32":
		return "float"
	case "float64":
		return "double"
	case "time.Time":
		if isDateFiled(tag) {
			return "date"
		}
		return "timestamp"
	default:
		return "Invalid type: " + typeName
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
func getPointersOfStructElements(basePoint unsafe.Pointer, selectFields []string, fieldsMap map[string]cassandraTableField) []interface{} {
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
		default:
			fieldsPtr = append(fieldsPtr, nil)
		}
	}
	return fieldsPtr
}

func appendPtr[T any](fieldsPtr *[]interface{}, basePoint unsafe.Pointer, field cassandraTableField) {
	if field.isPointer {
		var newVal T
		ptrAddress := unsafe.Add(basePoint, field.offSet)
		*(**T)(ptrAddress) = &newVal
		*fieldsPtr = append(*fieldsPtr, &newVal)
		return
	}
	*fieldsPtr = append(*fieldsPtr, (*T)(unsafe.Add(basePoint, field.offSet)))
}
