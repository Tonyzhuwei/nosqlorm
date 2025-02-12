package nosqlorm

import (
	"errors"
	"fmt"
	"github.com/gocql/gocql"
	"log"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"
	"unsafe"
)

const cqlTAG = "cql"
const jsonTAG = "json"

var modelCache sync.Map

type tableSchema struct {
	fields   []string
	fieldMap map[string]tableField
}

type tableField struct {
	fieldName       string
	isPartitionKey  bool
	isClusteringKey bool
	isStatic        bool
	dataType        reflect.Kind
	isPointer       bool
	isList          bool
	offSet          uintptr
}

type CqlOrm[T interface{}] struct {
	sess *gocql.Session
}

// NewCqlOrm Create a new object to access specific Cassandra table
func NewCqlOrm[T interface{}](session *gocql.Session) (*CqlOrm[T], error) {
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

			// Validate tag
			fieldName := getFieldName(tag)
			if fieldName == "" {
				return nil, errors.New("invalid CQL Tag: must have a json name")
			}
			if !isValidCqlTag(tag) {
				return nil, errors.New(fmt.Sprintf("Invalid CQL Tag for filed %s: %s", fieldName, tag.Get(cqlTAG)))
			}

			// Validate whether it is valid type
			dbType, isPointer, err := getFieldDBType(field.Type.String(), isDateFiled(tag))
			if err != nil {
				log.Fatal("Invalid field type " + field.Type.String())
				return nil, err
			}
			fieldType := field.Type.Kind()
			if isPointer {
				fieldType = field.Type.Elem().Kind()
			}
			if strings.HasPrefix(dbType, "list") {
				fieldType = field.Type.Elem().Kind()
			}

			schema.fields = append(schema.fields, fieldName)
			schema.fieldMap[fieldName] = tableField{
				fieldName:       fieldName,
				isPartitionKey:  isPartitionKey(tag),
				isClusteringKey: isClusterKey(tag),
				isStatic:        isStaticFiled(tag),
				dataType:        fieldType,
				isPointer:       isPointer,
				isList:          strings.HasPrefix(dbType, "list"),
				offSet:          field.Offset,
			}
		}
		modelCache.Store(typName, schema)
	}

	orm := &CqlOrm[T]{sess: session}
	return orm, nil
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
			if filedName == "-" {
				continue
			}
			filedDBType, _, err := getFieldDBType(typ.Field(i).Type.String(), isDateFiled(tag))
			if err != nil {
				return err
			}

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

func (ctx *CqlOrm[T]) Insert(obj T) error {
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
		if tableFields[i] == "-" {
			continue
		}
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

func (ctx *CqlOrm[T]) Select(obj T) ([]T, error) {
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
		if fieldName == "-" {
			continue
		}
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

func (ctx *CqlOrm[T]) Update(obj T) error {
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
		if filedName == "-" {
			continue
		}
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

func (ctx *CqlOrm[T]) Delete(obj T) error {
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
		if fieldName == "-" {
			continue
		}
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
func getFieldDBType(typeName string, isDate bool) (string, bool, error) {
	isPointer := false
	if typeName[0:1] == "*" {
		typeName = typeName[1:]
		isPointer = true
	}
	if typeName[0:2] == "[]" {
		typeName = typeName[2:]
		listDateType, _, _ := getFieldDBType(typeName, isDate)
		return fmt.Sprintf("list<%s>", listDateType), isPointer, nil
	}

	switch typeName {
	case "bool":
		return "boolean", isPointer, nil
	case "string":
		return "text", isPointer, nil
	case "int8":
		return "tinyint", isPointer, nil
	case "int16":
		return "smallint", isPointer, nil
	case "int32":
		return "int", isPointer, nil
	case "int":
		return "bigint", isPointer, nil
	case "int64":
		return "bigint", isPointer, nil
	case "float32":
		return "float", isPointer, nil
	case "float64":
		return "double", isPointer, nil
	case "time.Time":
		if isDate {
			return "date", isPointer, nil
		}
		return "timestamp", isPointer, nil
	default:
		return "", isPointer, errors.New("Invalid type: " + typeName)
	}
}

func isValidCqlTag(tag reflect.StructTag) bool {
	fieldName := tag.Get(jsonTAG)
	tagStr := tag.Get(cqlTAG)

	if fieldName == "-" && tagStr != "" {
		log.Fatal("Json ignored field should not contain CQL tag: ", tagStr)
		return false
	}

	if tagStr != "" {
		keys := strings.Split(tagStr, ",")
		allowKeys := []string{"pk", "ck", "static", "date"}
		for _, key := range keys {
			if !slices.Contains(allowKeys, key) {
				log.Fatal(fmt.Sprintf("Invalid tag: %s, key word %s is not allowed", tagStr, key))
				return false
			}
		}
		isPk := isPartitionKey(tag)
		isCk := isClusterKey(tag)
		isStatic := isStaticFiled(tag)
		if isPk && isCk {
			log.Fatal(fmt.Sprintf("Invalid tag: %s, a field could not be both clustering key and partition key", tagStr))
			return false
		}
		if (isPk || isCk) && isStatic {
			log.Fatal(fmt.Sprintf("Invalid tag: %s, static field could not be part of primary key", tagStr))
			return false
		}
	}
	return true
}

func isPartitionKey(tag reflect.StructTag) bool {
	return strings.Contains(tag.Get(cqlTAG), "pk")
}

func isClusterKey(tag reflect.StructTag) bool {
	return strings.Contains(tag.Get(cqlTAG), "ck")
}

func isStaticFiled(tag reflect.StructTag) bool {
	return strings.Contains(tag.Get(cqlTAG), "static")
}

func isDateFiled(tag reflect.StructTag) bool {
	return strings.Contains(tag.Get(cqlTAG), "date")
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
			appendPtr[string](&fieldsPtr, basePoint, field)
		case reflect.Struct:
			appendPtr[time.Time](&fieldsPtr, basePoint, field) // Only allow time.Time struct
		default:
			fieldsPtr = append(fieldsPtr, nil)
		}
	}
	return fieldsPtr
}

func appendPtr[T any](fieldsPtr *[]interface{}, basePoint unsafe.Pointer, field tableField) {
	if field.isList {
		if !field.isPointer {
			*fieldsPtr = append(*fieldsPtr, (*[]T)(unsafe.Add(basePoint, field.offSet)))
		} else {
			*fieldsPtr = append(*fieldsPtr, (**[]T)(unsafe.Add(basePoint, field.offSet)))
		}
		return
	}
	if field.isPointer {
		*fieldsPtr = append(*fieldsPtr, (**T)(unsafe.Add(basePoint, field.offSet)))
		return
	}
	*fieldsPtr = append(*fieldsPtr, (*T)(unsafe.Add(basePoint, field.offSet)))
}
