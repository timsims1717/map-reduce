package mapreduce

import (
	"database/sql"
	"fmt"
	"hash/fnv"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
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

type Task struct {
	Mappy MapTask
	Reducey ReduceTask
}

type Pair struct {
    Key   string
    Value string
}

type Interface interface {
    Map(key, value string, output chan<- Pair) error
    Reduce(key string, values <-chan string, output chan<- Pair) error
}

type Nothing struct {}

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

func Start(c Interface) error {
	argc := len(os.Args)
	if argc < 3 {
		fmt.Println("Usage: ", os.Args[0], "m/w <port> [masterAddress]")
		os.Exit(1)
	}
	role := os.Args[1]
	if role == "w" {
		return startWorker(os.Args[2], os.Args[3], c)
	} else {
		return startMaster(os.Args[2])
	}
}


func startWorker(port, masterAddress string, c Interface) error {
	address := fmt.Sprintf("%s:%s", getLocalAddress(), port)
	tempDir := "data"
	//http server//
	go startHTTPServer(address)
	working := false

	//Get work from the master//
	for {
		reply := new(Task)
		err := call(masterAddress, "Master.WorkRequest", Nothing{}, reply)
		if err != nil {
			if !working {
				log.Printf("Error while asking for work: %v\n", err)
				return err
			} else {
				log.Println("We're done here!")
				return nil
			}
		}
		working = true
		if reply.Mappy.M == 0 && reply.Reducey.M == 0 {
			log.Println("No work for me. Sleeping.")
			time.Sleep(time.Second)
		} else if reply.Mappy.M != 0 {
			log.Println("Mappy time!")
			//Process map tasks//
			err = reply.Mappy.Process(tempDir, c)
			if err != nil {
				//die
				log.Printf("THE MAPPY IS DEAD because of %v\n", err)
				return err
			}
			log.Println("Done mapping!")

			call(masterAddress, "Master.FinishedWork", address, reply)
		} else {
			log.Println("Reducey time!")
			//Process reduce tasks//
			err = reply.Reducey.Process(tempDir, c)
			if err != nil {
				//die
				log.Printf("THE REDUCEY IS DEAD because of %v\n", err)
				return err
			}
			log.Println("Done reducing!")

			call(masterAddress, "Master.FinishedWork", address, reply)
		}

	}
	return nil

}

func startMaster(port string) error {
	address := fmt.Sprintf("%s:%s", getLocalAddress(), port)
	log.Printf("Ready to listen at address %s!\n", address)

	M := 9
	R := 3
	source := "austen.sqlite3"
	sourcePattern := "data/map_%d_source.sqlite3"
	tempDir := "data"
	// os.RemoveAll(tempDir)

	go startHTTPServer(address)

	_, err := SplitDatabase(source, sourcePattern, M)
	if err != nil {
		log.Printf("Error while splitting databases: %v\n", err)
		return err
	}

	requests := make(chan bool)
	responses := make(chan *Task)
	finished := make(chan string, M)
	master := &Master {
		Request: requests,
		Response: responses,
		Finished: finished,
	}
	rpc.Register(master)
	rpc.HandleHTTP()

	m := 0
	for m < M {
		<-requests
		log.Printf("Received map request number %d!\n", m)
		response := &Task {
			Mappy: MapTask {
				M: M,
				R: R,
				N: m,
				SourceHost: makeURL(fmt.Sprintf("%s/%s", address, tempDir), mapSourceFile(m)),
			},
			Reducey: ReduceTask{},
		}
		responses <- response
		m++
	}
	mapAddresses := make([]string, M)
	m = 0
	for m < M {
		select {
		case madd := <-finished:
			log.Printf("Map worker %d (at address %s) finished.\n", m, madd)
			mapAddresses[m] = madd
			m++
		case <-requests:
			response := &Task {
				Mappy: MapTask{},
				Reducey: ReduceTask{},
			}
			responses <- response
		}
	}
	r := 0
	for r < R {
		m = 0
		sourceHosts := make([]string, M)
		for m < M {
			sourceHosts[m] = makeURL(fmt.Sprintf("%s/%s", mapAddresses[m], tempDir), mapOutputFile(m, r))
			m++
		}
		<-requests
		response := &Task {
			Mappy: MapTask{},
			Reducey: ReduceTask {
				M: M,
				R: R,
				N: r,
				SourceHosts: sourceHosts,
			},
		}
		responses <- response
		r++
	}
	reduceAddresses := make([]string, R)
	r = 0
	for r < R {
		select {
		case radd := <-finished:
			log.Printf("Reduce worker %d (at address %s) finished.\n", r, radd)
			reduceAddresses[r] = makeURL(fmt.Sprintf("%s/%s", radd, tempDir), reduceOutputFile(r))
			r++
		case <-requests:
			response := &Task {
				Mappy: MapTask{},
				Reducey: ReduceTask{},
			}
			responses <- response
		}
	}
	MergeDatabases(reduceAddresses, "totally_awesome_output_file.sqlite3", "temp")
	log.Println("All done!")
	return nil
}

type Master struct {
	Request chan<- bool
	Response <-chan *Task
	Finished chan<- string
}

func (m *Master) WorkRequest(request Nothing, response *Task) error {
	m.Request <- true
	task := <-m.Response
	response.Mappy = task.Mappy
	response.Reducey = task.Reducey
	return nil
}

func (m *Master) FinishedWork(request string, response *Nothing) error {
	m.Finished <- request
	return nil
}

func getLocalAddress() string {
	var localaddress string

	ifaces, err := net.Interfaces()
	if err != nil {
		panic("init: failed to find network interfaces")
	}

	// find the first non-loopback interface with an IP address
	for _, elt := range ifaces {
		if elt.Flags&net.FlagLoopback == 0 && elt.Flags&net.FlagUp != 0 {
			addrs, err := elt.Addrs()
			if err != nil {
				panic("init: failed to get addresses for network interface")
			}

			for _, addr := range addrs {
				if ipnet, ok := addr.(*net.IPNet); ok {
					if ip4 := ipnet.IP.To4(); len(ip4) == net.IPv4len {
						localaddress = ip4.String()
						break
					}
				}
			}
		}
	}
	if localaddress == "" {
		panic("init: failed to find non-loopback interface with valid address on this node")
	}

	return localaddress
}

func startHTTPServer(address string) {
	http.Handle("/data/", http.StripPrefix("/data", http.FileServer(http.Dir("./data"))))
	if err := http.ListenAndServe(address, nil); err != nil {
		log.Fatalf("Error in HTTP server for %s: %v", address, err)
	}
}

func call(address string, method string, request interface{}, reply interface{}) error {
	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		return err
	}
	defer client.Close()
	return client.Call(method, request, reply)
}