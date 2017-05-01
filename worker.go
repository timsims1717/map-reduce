package mapreduce

import (
	"database/sql"
	"fmt"
	"hash/fnv"
	"log"
	"net/http"
	"os"
	// "path/filepath"
	"strings"
	"strconv"
	"unicode"
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
func makeURL(host, file string) string { return fmt.Sprintf("http://%s/%s", host, file) }

func (task *MapTask) Process(tempdir string, client Interface) error {
	// Download and open the input file
	inputFile := fmt.Sprintf("%s/%s", tempdir, mapInputFile(task.N))
	err := Download(task.SourceHost, inputFile)
	if err != nil {
		log.Printf("Error while downloading %s: %v", task.SourceHost, err)
		return err
	}
	datbase, err := OpenDatabase(inputFile)
	if err != nil {
		log.Printf("Error while opening %s: %v", inputFile, err)
		return err
	}
	defer datbase.Close()

	// Create output files
	filenames := make([]string, task.R)
	dats := make([]*sql.DB, task.R)
	inserts := make([]*sql.Stmt, task.R)
	cmd := `
	insert into pairs (key, value) values (?, ?);
	`
	for r, _ := range(dats) {
		filename := fmt.Sprintf("%s/%s", tempdir, mapOutputFile(task.N, r))
		filenames[r] = filename
		dats[r], err = CreateDatabase(filename)
		if err != nil {
			log.Printf("Error while creating %s: %v", filename, err)
			return err
		}
		defer dats[r].Close()
		inserts[r], err = dats[r].Prepare(cmd)
		if err != nil {
			log.Printf("Error while preparing statement for %s: %v", filename, err)
			return err
		}
		defer inserts[r].Close()
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
	proc := 0
	gen := 0
	var k, v string
	for rows.Next() {
		proc++
		err = rows.Scan(&k, &v)
		if err != nil {
			log.Printf("Error while reading values from %s: %v", inputFile, err)
			return err
		}
		outChan := make(chan Pair)
		go client.Map(k, v, outChan)
		for pair := range outChan {
			gen++
			hash := fnv.New32() // from the stdlib package hash/fnv
			hash.Write([]byte(pair.Key))
			r := int(hash.Sum32()) % task.R
			_, err = inserts[r].Exec(pair.Key, pair.Value)
			if err != nil {
				log.Printf("Error while inserting pair into %s: %v", filenames[r], err)
				return err
			}
		}
	}
	fmt.Printf("map task processed %d pairs, generated %d pairs\n", proc, gen)
	return nil
}

func (task *ReduceTask) Process(tempdir string, client Interface) error {
	// Create input database and merge files
	inputFilename := fmt.Sprintf("%s/%s", tempdir, reduceInputFile(task.N))
	inputDatabase, err := MergeDatabases(task.SourceHosts, inputFilename, reduceTempFile(task.N))
	if err != nil {
		log.Printf("Error while opening %s: %v", inputFilename, err)
		return err
	}
	defer inputDatabase.Close()

	// Create output database
	outputFilename := fmt.Sprintf("%s/%s", tempdir, reduceOutputFile(task.N))
	outputDatabase, err := CreateDatabase(outputFilename)
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
	keyCount := 0
	valueCount := 0
	gen := 0
	var k, v, currentK string
	var values chan string
	var output chan Pair
	i := 0
	for rows.Next() {
		valueCount++
		err = rows.Scan(&k, &v)
		if err != nil {
			log.Printf("Error while reading values from %s: %v", inputFilename, err)
			return err
		}
		if i != 0 && currentK != k {
			close(values)
			for pair := range output {
				gen++
				_, err = insert.Exec(pair.Key, pair.Value)
				if err != nil {
					log.Printf("Error while inserting pair into %s: %v", outputFilename, err)
					return err
				}
			}
		}
		if i == 0 || currentK != k {
			keyCount++
			currentK = k
			values = make(chan string)
			output = make(chan Pair)
			go client.Reduce(k, values, output)
		}
		values <- v
		i++
	}
	close(values)
	for pair := range output {
		gen++
		_, err = insert.Exec(pair.Key, pair.Value)
		if err != nil {
			log.Printf("Error while inserting pair into %s: %v", outputFilename, err)
			return err
		}
	}
	fmt.Printf("reduce task processed %d keys and %d values, generated %d pairs\n", keyCount, valueCount, gen)
	return nil
}

// READY TO TEST PART 2

func Start() {
	args := os.Args

	role := args[1]
	if role == "W" {
		startWorker(args[2])
	} else {
		startMaster(args[2])
	}

	log.Println("Ready!")
	M := 9
	R := 3
	address := "localhost:8080"
	source := "austen.sqlite3"
	outputPattern := "data/map_%d_source.sqlite3"
	tempDir := "data"
	// os.RemoveAll(tempDir)

	go func() {
		http.Handle("/data/", http.StripPrefix("/data", http.FileServer(http.Dir("./data"))))
		if err := http.ListenAndServe(address, nil); err != nil {
			log.Fatalf("Error in HTTP server for %s: %v", address, err)
		}
	}()

	_, err := SplitDatabase(source, outputPattern, M)
	if err != nil {
		log.Fatalf("Error while splitting databases: %v", err)
	}

	i := 0
	finished := make(chan bool, 100)
	for i < M {
		go func(m int) {
			c := new(Client)
			task := &MapTask{
				M, R, m, makeURL(fmt.Sprintf("%s/%s", address, tempDir), mapSourceFile(m)),
			}
			task.Process(tempDir, c)
			finished <- true
		}(i)
		i++
	}
	i = 0
	for i < M {
		<-finished
		i++
	}
	i = 0
	for i < R {
		m := 0
		sourceHosts := make([]string, M)
		for m < M {
			sourceHosts[m] = makeURL(fmt.Sprintf("%s/%s", address, tempDir), mapOutputFile(m, i))
			m++
		}
		go func(r int) {
			c := new(Client)
			task := &ReduceTask{
				M: M, R: R, N: r, SourceHosts: sourceHosts,
			}
			task.Process(tempDir, c)
			finished <- true
		}(i)
		i++
	}
	i = 0
	for i < R {
		<-finished
		i++
	}
	log.Println("All done!")
}

type Client struct{}

func (c Client) Map(key, value string, output chan<- Pair) error {
	defer close(output)
	lst := strings.Fields(value)
	for _, elt := range lst {
		word := strings.Map(func(r rune) rune {
			if unicode.IsLetter(r) || unicode.IsDigit(r) {
				return unicode.ToLower(r)
			}
			return -1
		}, elt)
		if len(word) > 0 {
			output <- Pair{Key: word, Value: "1"}
		}
	}
	return nil
}

func (c Client) Reduce(key string, values <-chan string, output chan<- Pair) error {
	defer close(output)
	count := 0
	for v := range values {
		i, err := strconv.Atoi(v)
		if err != nil {
			return err
		}
		count += i
	}
	p := Pair{Key: key, Value: strconv.Itoa(count)}
	output <- p
	return nil
}
