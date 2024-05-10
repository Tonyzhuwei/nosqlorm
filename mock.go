package nosqlorm

import (
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

type MockSession struct {
	Test         *testing.T
	Expectations []interface{}
	idx          int
	lock         sync.Mutex
}

type expect[T interface{}] struct {
	ExpectInput []T
	returnObj   []T // For Select only
	errorObj    error
}

type MockTable[T interface{}] struct {
	sess *MockSession
	t    *testing.T
}

func NewMockSession(t *testing.T) *MockSession {
	return &MockSession{Test: t, Expectations: make([]interface{}, 0), idx: 0}
}

func (m *MockSession) AddIdx() {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.idx++
}

func NewMockTable[T interface{}](sess *MockSession) *MockTable[T] {
	return &MockTable[T]{sess: sess, t: sess.Test}
}

func (m *MockTable[T]) Select(obj T) ([]T, error) {
	idx := m.sess.idx
	defer m.sess.AddIdx()
	if idx >= len(m.sess.Expectations) {
		m.t.Error("Unexpected query")
	}
	assert.Equal(m.t, obj, m.sess.Expectations[idx].(expect[T]).ExpectInput[0])
	return m.sess.Expectations[idx].(expect[T]).returnObj, nil
}

func (m *MockTable[T]) Insert(obj T) error {
	idx := m.sess.idx
	defer m.sess.AddIdx()
	if idx >= len(m.sess.Expectations) {
		m.t.Error("Unexpected query")
	}
	assert.Equal(m.t, obj, m.sess.Expectations[idx].(expect[T]).ExpectInput[0])
	return m.sess.Expectations[idx].(expect[T]).errorObj
}

func (m *MockTable[T]) Update(obj T) error {
	idx := m.sess.idx
	defer m.sess.AddIdx()
	if idx >= len(m.sess.Expectations) {
		m.t.Error("Unexpected query")
	}
	assert.Equal(m.t, obj, m.sess.Expectations[idx].(expect[T]).ExpectInput[0])
	return m.sess.Expectations[idx].(expect[T]).errorObj
}

func (m *MockTable[T]) Delete(obj T) error {
	idx := m.sess.idx
	defer m.sess.AddIdx()
	if idx >= len(m.sess.Expectations) {
		m.t.Error("Unexpected query")
	}
	assert.Equal(m.t, obj, m.sess.Expectations[idx].(expect[T]).ExpectInput[0])
	return m.sess.Expectations[idx].(expect[T]).errorObj
}

func (m *MockTable[T]) AddSelectExpectation(input T, objs []T) {
	newExpect := expect[T]{
		ExpectInput: []T{input},
		returnObj:   objs,
		errorObj:    nil,
	}
	m.sess.Expectations = append(m.sess.Expectations, newExpect)
}

func (m *MockTable[T]) AddOtherExpectation(input T, err error) {
	newExpect := expect[T]{
		ExpectInput: []T{input},
		returnObj:   nil,
		errorObj:    err,
	}
	m.sess.Expectations = append(m.sess.Expectations, newExpect)
}
