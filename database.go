package main

import (
    //"bufio"
	"database/sql"
    "fmt"
    _ "github.com/mattn/go-sqlite3"
    "io"
    //"io/ioutil"
    "log"
    "net/http"
    "os"
)

func openDatabase (path string) (*sql.DB, error) {
	dat, err := sql.Open("sqlite3", path)

	if err != nil || dat == nil {
		return nil, err
	}

	cmd := `
	pragma synchronous = off;
	pragma journal_mode = off;
	`

	_, err = dat.Exec(cmd)
	if err != nil {
		dat.Close()
		return nil, err
	}
	return dat, nil
}

func createDatabase(path string) (*sql.DB, error) {
	if _, err := os.Stat(path); err == nil {
		os.Remove(path)
	}
	_, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	dat, err := sql.Open("sqlite3", path)

	if err != nil || dat == nil {
		return nil, err
	}

	cmd := `
	pragma synchronous = off;
	pragma journal_mode = off;
	create table pairs (key text, value text);
	`

	_,err = dat.Exec(cmd)
	if err != nil {
		dat.Close()
		return nil, err
	}
	return dat, nil
}

func splitDatabase(source, outputPattern string, m int) ([]string, error) {
	dat, err := openDatabase(source)
	if err != nil || dat == nil {
		return nil, err
	}
	defer dat.Close()

	cmd := `
	select count(1) from pairs;
	`
	row, err := dat.Query(cmd)
	if err != nil {
		return nil, err
	}
	defer row.Close()
	var i int;
	if row.Next() {
		err = row.Scan(&i)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, fmt.Errorf("No count returned from table pairs")
	}
	if i < m {
		return nil, fmt.Errorf("Number of pairs < Number of Map tasks: i = %d, m = %d", i, m)
	}

	cmd = `
	insert into pairs (key, value) values (?, ?);
	`
	filenames := make([]string, m)
	dats := make([]*sql.DB, m)
	inserts := make([]*sql.Stmt, m)
	for i, _ := range(dats) {
		filename := fmt.Sprintf(outputPattern, i)
		filenames = append(filenames, filename)
		dats[i], err = createDatabase(filename)
		if err != nil {
			return nil, err
		}
		defer dats[i].Close()
		inserts[i], err = dats[i].Prepare(cmd)
		if err != nil {
			return nil, err
		}
		defer inserts[i].Close()
	}

	cmd = `
	select key, value from pairs;
	`
	rows, err := dat.Query(cmd)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var k, v string
	i = 0
	for rows.Next() {
		err = rows.Scan(&k, &v)
		if err != nil {
			return nil, err
		}
		_, err = inserts[i].Exec(k, v)
		if err != nil {
			return nil, err
		}
		i = (i + 1) % m
	}

	return filenames, nil
}

func mergeDatabases(urls []string, path string, temp string) (*sql.DB, error) {
	// datbase, err := createDatabase(path)
	_, err := createDatabase(path) // I changed this because it complained that datbase was unused. Just change it back whenever
	if err != nil {
		log.Fatal(err)
	}

	for _, url := range urls {
		err := download(url, temp)
		if err != nil {
			log.Fatal(err)
		}
	}

	//NOT FINISHED
	return nil, nil
}

func download(url, path string) error {

	out, err := os.Create(path)
  	if err != nil  {
    	return err
  	}
  	defer out.Close()

	resp, err := http.Get(url)
	if err != nil {
			log.Fatal(err)
		}
	defer resp.Body.Close()

	_, err = io.Copy(out, resp.Body)

	return err
}

func main () {
	_,err := splitDatabase("austen.sqlite3", "result-%d.sqlite3", 20)
	if err != nil {
		fmt.Println(err)
	}
}