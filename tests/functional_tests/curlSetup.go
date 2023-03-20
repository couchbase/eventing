package eventing

import (
	"log"
	"net/http"
)

func curlSetup() {
	server := &http.Server{Addr: ":9090"}
	http.HandleFunc("/empty", emptyHandler)
	http.HandleFunc("/large", largeHandler)
	http.HandleFunc("/get", getOrDeleteHandler)
	http.HandleFunc("/get/", getOrDeleteHandler)
	http.HandleFunc("/getRedirect", getRedirectHandler)
	http.HandleFunc("/getRedirect/", getRedirectHandler)
	http.HandleFunc("/delete", getOrDeleteHandler)
	http.HandleFunc("/delete/", getOrDeleteHandler)
	http.HandleFunc("/post", postOrPutHandler)
	http.HandleFunc("/post/", postOrPutHandler)
	http.HandleFunc("/put", postOrPutHandler)
	http.HandleFunc("/put/", postOrPutHandler)
	http.HandleFunc("/head", headHandler)
	http.HandleFunc("/head/", headHandler)
	http.HandleFunc("/parseQueryParams/", parseQueryParamsHandler)
	go func() {
		err := server.ListenAndServe()
		if err != nil {
			log.Printf("Unable to start the HTTP Server, err : %v", err)
		}
	}()
}
