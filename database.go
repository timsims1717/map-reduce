package main

import (
    //"bufio"
	"database/sql"
    //"fmt"
    _ "github.com/mattn/go-sqlite3"
    "io"
    //"io/ioutil"
    "log"
    "net/http"
    "os"
)

func openDatabase (path string) (*sql.DB, error) {
	dat, err := sql.Open( "sqlite3", path)

	if err != nil || dat == nil {
		return nil, err
	}

	cmd := `
	pragma synchronous = off;
	pragma journal_mode = off;
	`

	_,err = dat.Exec(cmd)
	if err != nil {
		dat.Close()
		return nil, err
	}
	return dat, nil
}

func createDatabase(path string) (*sql.DB, error) {
	if _,err := os.Stat(path); err == nil {
		os.Remove(path)
	}
	_, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	dat, err := sql.Open( "sqlite3", path)


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
	dat,err := openDatabase(source)

	if err != nil || dat == nil {
		return nil, err
	}

	cmd := `
	select count(1) from pairs
	`

	rows,err := dat.Query(cmd)
	if err != nil {
		dat.Close()
		return nil, err
	}
	var i int;
	if rows.Next() {
		err = rows.Scan(&i)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, err
	}

	if i < m {
		return nil, err
	}
	//NOT FINISHED
	return nil, nil
}

func mergeDatabases(urls []string, path string, temp string) (*sql.DB, error) {
	datbase, err := createDatabase(path)
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
	//_,err := openDatabase("austen.sqlite3")
	//_,err := createDatabase("datbase.sqlite3")
	// if err != nil {
	// 	fmt.Println(err)
	// }
}
