package main

import (
	"flag"
	"fmt"
	"github.com/Tonyzhuwei/nosqlorm"
	"github.com/gocql/gocql"
	"log"
	"os"
	"runtime/pprof"
	"time"
)

type Person struct {
	Name        string     `json:"name" cql:"pk"`
	Age         int8       `json:"age" cql:"ck"`
	Address     string     `json:"address"`
	Income      *float64   `json:"-"`
	LuckyNumber []string   `json:"lucky_number"`
	CreatedTime *time.Time `json:"created_time"`
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
	Name:        "Tony",
	Age:         30,
	Address:     "this is a test address",
	Income:      nosqlorm.GetPointer[float64](10000.10),
	LuckyNumber: []string{"abc", "efg"},
	CreatedTime: nosqlorm.GetPointer(time.Now()),
}

var cpuprofile = flag.String("cpuprofile", "pprof.profile", "write cpu profile to file")

func main() {
	// Create Cassandra connect session.
	clustser := gocql.NewCluster("localhost:9042")
	clustser.Keyspace = "cqlorm"
	sess, err := clustser.CreateSession()
	if err != nil {
		panic(err)
	}

	// Create tables if not existing
	err = nosqlorm.CreateCassandraTables(sess, Person{})
	if err != nil {
		panic(err)
	}

	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	tableCtx, err := nosqlorm.NewCqlOrm[Person](sess)
	if err != nil {
		panic(err)
	}

	app := App{
		personTable: tableCtx,
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

	//Delete
	err = app.DeletePerson(testPerson)
	if err != nil {
		panic(err)
	}
	fmt.Println("Delete record: ", testPerson)
}
