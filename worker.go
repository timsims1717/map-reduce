package main

import (
    //"bufio"
	//"database/sql"
    "fmt"
    _ "github.com/mattn/go-sqlite3"
    //"io"
    //"io/ioutil"
    "log"
    //"net/http"
    //"os"
)

//Useful Structs//
type MapTask struct {
    M, R       int    // total number of map and reduce tasks
    N          int    // map task number, 0-based
    SourceHost string // address of host with map input file
}

type ReduceTask struct {
    M, R        int      // total number of map and reduce tasks
    N           int      // reduce task number, 0-based
    SourceHosts []string // addresses of map workers
}

type Pair struct {
    Key   string
    Value string
}

type Interface interface {
    Map(key, value string, output chan<- Pair) error
    Reduce(key string, values <-chan string, output chan<- Pair) error
}

//Helpful Helpers!//
func mapSourceFile(m int) string       { return fmt.Sprintf("map_%d_source.sqlite3", m) }
func mapInputFile(m int) string        { return fmt.Sprintf("map_%d_input.sqlite3", m) }
func mapOutputFile(m, r int) string    { return fmt.Sprintf("map_%d_output_%d.sqlite3", m, r) }
func reduceInputFile(r int) string     { return fmt.Sprintf("reduce_%d_input.sqlite3", r) }
func reduceOutputFile(r int) string    { return fmt.Sprintf("reduce_%d_output.sqlite3", r) }
func reducePartialFile(r int) string   { return fmt.Sprintf("reduce_%d_partial.sqlite3", r) }
func reduceTempFile(r int) string      { return fmt.Sprintf("reduce_%d_temp.sqlite3", r) }
func makeURL(host, file string) string { return fmt.Sprintf("http://%s/data/%s", host, file) }
//

func (task *MapTask) Process(tempdir string, client Interface) error {
	db, err := createDatabase(tempdir)
	if err != nil {
		log.Fatal(err)
	}

	db, err = openDatabase(tempdir)
	if err != nil {
		log.Fatal(err)
	}

	db, err = splitDatabase("austen.sqlite3", "result-%d.sqlite3", 20)
	if err != nil {
		log.Fatal(err)
	}

	cmd := `
	select key, value from pairs;
	`

	rows, err := db.Query(cmd)
	if err != nil {
		return err
	}

	for rows.Next() {
		// p Pair = {
		// 	Key = rows.key
		// 	Value = rows.value
		// }
		//client.Map(rows.key, rows.value)
	}

	return nil
}

func (task *ReduceTask) Process(tempdir string, client Interface) error {
    return nil
}

// func main() {
// 	fmt.Printf("MAIN")
// }