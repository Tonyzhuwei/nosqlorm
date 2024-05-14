package nosqlorm

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_GetFieldDBType(t *testing.T) {
	type input struct {
		str    string
		isDate bool
	}
	type expect struct {
		str     string
		isPoint bool
		err     error
	}
	type testCase struct {
		input
		expect
	}
	testCases := make([]testCase, 0)
	testCases = append(testCases, testCase{
		input:  input{"int", false},
		expect: expect{"bigint", false, nil},
	})
	testCases = append(testCases, testCase{
		input:  input{"*int", false},
		expect: expect{"bigint", true, nil},
	})
	testCases = append(testCases, testCase{
		input:  input{"float32", false},
		expect: expect{"float", false, nil},
	})
	testCases = append(testCases, testCase{
		input:  input{"bool", false},
		expect: expect{"boolean", false, nil},
	})
	testCases = append(testCases, testCase{
		input:  input{"string", false},
		expect: expect{"text", false, nil},
	})
	testCases = append(testCases, testCase{
		input:  input{"*string", false},
		expect: expect{"text", true, nil},
	})
	testCases = append(testCases, testCase{
		input:  input{"time.Time", true},
		expect: expect{"date", false, nil},
	})
	testCases = append(testCases, testCase{
		input:  input{"time.Time", false},
		expect: expect{"timestamp", false, nil},
	})
	testCases = append(testCases, testCase{
		input:  input{"*time.Time", false},
		expect: expect{"timestamp", true, nil},
	})
	testCases = append(testCases, testCase{
		input:  input{"TestStruct", false},
		expect: expect{"", true, errors.New("Invalid type")},
	})
	testCases = append(testCases, testCase{
		input:  input{"[]int", false},
		expect: expect{"list<bigint>", false, nil},
	})
	testCases = append(testCases, testCase{
		input:  input{"[]string", false},
		expect: expect{"list<text>", false, nil},
	})
	testCases = append(testCases, testCase{
		input:  input{"[]time.Time", false},
		expect: expect{"list<timestamp>", false, nil},
	})
	testCases = append(testCases, testCase{
		input:  input{"[]time.Time", true},
		expect: expect{"list<date>", false, nil},
	})
	testCases = append(testCases, testCase{
		input:  input{"*[]time.Time", true},
		expect: expect{"list<date>", true, nil},
	})
	testCases = append(testCases, testCase{
		input:  input{"map[int]string", true},
		expect: expect{"list<date>", false, errors.New("Invalid type")},
	})

	for _, testcase := range testCases {
		typeStr, isPoint, err := getFieldDBType(testcase.input.str, testcase.input.isDate)
		if testcase.expect.err != nil {
			assert.Error(t, err, testcase.input.str)
		} else {
			assert.Equal(t, testcase.expect.str, typeStr, testcase.input.str)
			assert.Equal(t, testcase.expect.isPoint, isPoint, testcase.input.str)
			assert.Equal(t, testcase.expect.err, err, testcase.input.str)
		}
	}

}
