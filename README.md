# NosqlOrm
A lightweight Golang ORM tailored for NoSQL databases, featuring built-in mock support. Currently, it supports Cassandra, with plans to extend compatibility to additional databases in the future.
# Get Start
```
go get github.com/Tonyzhuwei/nosqlorm
```
# Usage
## Define Models
```
type Person struct {
Name    string `json:"name" cql:"pk"`
Age     int8   `json:"age" cql:"ck"`
Address string `json:"address"`
}
```
## Migrate Tables
```
// Create Cassandra connect session.
clustser := gocql.NewCluster("localhost:9042")
clustser.Keyspace = "cqlorm"
sess, err := clustser.CreateSession()
if err != nil {
    panic(err)
}

// Create tables if not existing
nosqlorm.MigrateCassandraTables(sess, Person{})
```
## CRUD
```
personCtx := nosqlorm.NewCqlOrm[Person](sess)
result, err := personCtx.Select(Person{
    Name: "tony",
    Age:  30,
})
```

## Mock DB Access
```
var testPerson = Person{
	Name:    "Tony",
	Age:     30,
	Address: "this is a test address",
}

sess := nosqlorm.NewMockSession(t)
mockPersonTable := nosqlorm.NewMockTable[Person](sess)
mockPersonTable.AddSelectExpectation(testPerson, []Person{testPerson})

mockPersonTable.Select(testPerson)
```


# Missing Parts:
- Enhance performance through cache utilization.
- Facilitate transactions across multiple tables simultaneously.
- Enhance mock functionality to include 'ignore' and other features.
- Optimize data scanning processes.
- Implement comprehensive unit test cases.
- Enable batch methods and corresponding mock support.
- Incorporate logging capabilities, record low-performance CQL queries.
