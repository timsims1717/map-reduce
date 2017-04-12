package main

func openData (path string) (*sql.DB, error) {
	dat, err := ioutil.ReadFile(path)
}