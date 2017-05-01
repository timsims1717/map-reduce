package main

import (
	"hash/fnv"
	"log"
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

func (task *MapTask) Process(tempdir string, client Interface) error {
	// Download and open the input file
	inputFile := mapInputFile(task.N)
	err := database.Download(makeURL(task.SourceHost, inputFile))
	if err != nil {
		log.Printf("Error while downloading %s: %v", inputFile, err)
		return err
	}
	datbase, err := database.OpenDatabase(inputFile)
	if err != nil {
		log.Printf("Error while opening %s: %v", inputFile, err)
		return err
	}
	defer datbase.Close()

	// Create output files
	filenames := make([]string, task.R)
	dats := make([]*sql.DB, task.R)
	inserts := make([]*sql.Stmt, tasks.R)
	cmd := `
	insert into pairs (key, value) values (?, ?);
	`
	for r, _ := range(dats) {
		filename := mapOutputFile(task.N, r)
		filenames[i] = filename
		dats[i], err = database.CreateDatabase(filename)
		if err != nil {
			log.Printf("Error while creating %s: %v", filename, err)
			return err
		}
		defer dats[i].Close()
		inserts[i], err = dats[i].Prepare(cmd)
		if err != nil {
			log.Printf("Error while preparing statement for %s: %v", filename, err)
			return err
		}
		defer inserts[i].Close()
	}

	// Select All Pairs from source
	cmd = `
	select key, value from pairs;
	`
	rows, err := datbase.Query(cmd)
	if err != nil {
		log.Printf("Error while selecting all pairs from %s: %v", inputFile, err)
		return err
	}
	defer rows.Close()

	// For each pair...
	var k, v string
	for rows.Next() {
		err = rows.Scan(&k, &v)
		if err != nil {
			log.Printf("Error while reading values from %s: %v", inputFile, err)
			return err
		}
		outChan := make(chan Pair)
		go client.Map(k, v, outChan)
		for pair := range(outChan) {
			hash := fnv.New32() // from the stdlib package hash/fnv
			hash.Write([]byte(pair.Key))
			r := int(hash.Sum32()) % task.R
			_, err = inserts[r].Exec(k, v)
			if err != nil {
				log.Printf("Error while inserting pair into %s: %v", filenames[r], err)
				return err
			}
		}
	}
}

func (task *ReduceTask) Process(tempdir string, client Interface) error {
	// Create input database and merge files
	inputFilename := reduceInputFile(task.N)
	inputDatabase, err := database.MergeDatabases(task.SourceHosts, inputFilename, reduceTempFile(task.N))
	if err != nil {
		log.Printf("Error while opening %s: %v", inputFilename, err)
		return err
	}
	defer inputDatabase.Close()

	// Create output database
	outputFilename := reduceOutputFile(task.N)
	outputDatabase, err := database.CreateDatabase(outputFilename)
	if err != nil {
		log.Printf("Error while opening %s: %v", outputFilename, err)
		return err
	}
	defer outputDatabase.Close()
	cmd := `
	insert into pairs (key, value) values (?, ?);
	`
	insert, err := outputDatabase.Prepare(cmd)
	if err != nil {
		log.Printf("Error while preparing statement for %s: %v", outputFilename, err)
		return err
	}
	defer insert.Close()

	// Select All Pairs from source in order
	cmd = `
	select key, value from pairs order by key, value;
	`
	rows, err := inputDatabase.Query(cmd)
	if err != nil {
		log.Printf("Error while selecting all pairs from %s: %v", inputFilename, err)
		return err
	}
	defer rows.Close()

	// For each pair...
	var k, v, currentK string
	var values chan string
	var output chan Pair
	i = 0
	for rows.Next() {
		err = rows.Scan(&k, &v)
		if err != nil {
			log.Printf("Error while reading values from %s: %v", inputFilename, err)
			return err
		}
		if i != 0 && currentK != k {
			close(values)
			for pair := range(output) {
				_, err = insert.Exec(k, v)
				if err != nil {
					log.Printf("Error while inserting pair into %s: %v", outputFilename, err)
					return err
				}
			}
		}
		if i == 0 || currentK != k {
			currentK = k
			values = make(chan string)
			output = make(chan Pair)
			go client.Reduce(k, values, output)
		}
		values <- v
		i++
	}
	close(values)
	for pair := range(output) {
		_, err = insert.Exec(k, v)
		if err != nil {
			log.Printf("Error while inserting pair into %s: %v", outputFilename, err)
			return err
		}
	}
}

// READY TO TEST PART 2
