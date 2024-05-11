package main

import (
	"flag"
	"fmt"
	"github.com/Tonyzhuwei/nosqlorm"
	"github.com/gocql/gocql"
	"log"
	"os"
	"runtime/pprof"
)

type Person struct {
	Name    string `json:"name" cql:"pk"`
	Age     int8   `json:"age" cql:"ck"`
	Address string `json:"address"`
}

type App struct {
	personTable nosqlorm.NoSqlOrm[Person]
}

func (app *App) QueryPerson(person Person) ([]Person, error) {
	result, err := app.personTable.Select(person)
	return result, err
}

func (app *App) DeletePerson(person Person) error {
	err := app.personTable.Delete(person)
	if err != nil {
		fmt.Println(err)
	}
	return err
}

func (app *App) InsertPerson(person Person) error {
	err := app.personTable.Insert(person)
	if err != nil {
		fmt.Println(err)
	}
	return err
}

func (app *App) UpdatePerson(person Person) error {
	err := app.personTable.Update(person)
	if err != nil {
		fmt.Println(err)
	}
	return err
}

var testPerson = Person{
	Name:    "Tony",
	Age:     30,
	Address: "this is a test address",
}

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

func main() {
	// Create Cassandra connect session.
	clustser := gocql.NewCluster("localhost:9042")
	clustser.Keyspace = "cqlorm"
	sess, err := clustser.CreateSession()
	if err != nil {
		panic(err)
	}

	// Create tables if not existing
	nosqlorm.CreateCassandraTables(sess, Person{})

	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	app := App{
		personTable: nosqlorm.NewCqlOrm[Person](sess),
	}

	// Insert
	err = app.InsertPerson(testPerson)
	if err != nil {
		panic(err)
	}

	// Query, inside a method
	result, _ := app.QueryPerson(testPerson)
	fmt.Println("Query result: ", result)

	// Update
	updatePerson := testPerson
	updatePerson.Address = "Updated address"
	err = app.UpdatePerson(updatePerson)
	if err != nil {
		panic(err)
	}
	// Query after updated, Direct call select method
	result2, _ := app.personTable.Select(Person{Name: "Tony", Age: 30})
	fmt.Println("Query result after updated: ", result2)

	// Delete
	err = app.DeletePerson(testPerson)
	if err != nil {
		panic(err)
	}
	fmt.Println("Delete record: ", testPerson)
}
