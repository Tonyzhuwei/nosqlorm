package main

import (
	"nosqlorm"
	"testing"
)

func Test_Query(t *testing.T) {
	mockSess := nosqlorm.NewMockSession(t)
	mockPersonTable := nosqlorm.NewMockTable[Person](mockSess)
	mockApp := App{
		personTable: mockPersonTable,
	}
	mockPersonTable.AddSelectExpectation(testPerson, []Person{testPerson})

	mockApp.QueryPerson(testPerson)
}

func Test_Delete(t *testing.T) {
	sess := nosqlorm.NewMockSession(t)
	mockPersonTable := nosqlorm.NewMockTable[Person](sess)
	mockApp := App{
		personTable: mockPersonTable,
	}
	mockPersonTable.AddOtherExpectation(testPerson, nil)
	mockApp.DeletePerson(testPerson)
}

func Test_Insert(t *testing.T) {
	sess := nosqlorm.NewMockSession(t)
	mockPersonTable := nosqlorm.NewMockTable[Person](sess)
	mockApp := App{
		personTable: mockPersonTable,
	}
	mockPersonTable.AddOtherExpectation(testPerson, nil)
	mockApp.InsertPerson(testPerson)
}

func Test_Update(t *testing.T) {
	sess := nosqlorm.NewMockSession(t)
	mockPersonTable := nosqlorm.NewMockTable[Person](sess)
	mockApp := App{
		personTable: mockPersonTable,
	}
	mockPersonTable.AddOtherExpectation(testPerson, nil)
	mockApp.UpdatePerson(testPerson)
}
