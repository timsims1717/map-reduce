package main

	go func() {
    http.Handle("/data/", http.StripPrefix("/data", http.FileServer(http.Dir("/data"))))
    	if err := http.ListenAndServe("localhost:8080", nil); err != nil {
    	    log.Printf("Error in HTTP server for %s: %v", "localhost:8080", err)
   	 	}
	}()