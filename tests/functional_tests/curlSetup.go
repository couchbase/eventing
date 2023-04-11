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
	http.HandleFunc("/curlURLEncodeInlineQueryParams66X/", curlURLEncodeInlineQueryParams6_6_XHandler)
	http.HandleFunc("/curlURLEncodeQueryParamsAttrString66X/", curlURLEncodeQueryParamsAttrString6_6_XHandler)
	http.HandleFunc("/curlURLEncodeQueryParamsAttrObject66X/", curlURLEncodeQueryParamsAttrObject6_6_XHandler)
	http.HandleFunc("/curlURLEncodeInlineQueryParams71X/", curlURLEncodeInlineQueryParams7_1_XHandler)
	http.HandleFunc("/curlURLEncodeQueryParamsAttrString71X/", curlURLEncodeQueryParamsAttrString7_1_XHandler)
	http.HandleFunc("/curlURLEncodeQueryParamsAttrObject71X/", curlURLEncodeQueryParamsAttrObject7_1_XHandler)
	http.HandleFunc("/curlURLEncodeInlineQueryParams72X/", curlURLEncodeInlineQueryParams7_2_XHandler)
	http.HandleFunc("/curlURLEncodeQueryParamsAttrString72X/", curlURLEncodeQueryParamsAttrString7_2_XHandler)
	http.HandleFunc("/curlURLEncodeQueryParamsAttrObject72X/", curlURLEncodeQueryParamsAttrObject7_2_XHandler)
	go func() {
		err := server.ListenAndServe()
		if err != nil {
			log.Printf("Unable to start the HTTP Server, err : %v", err)
		}
	}()
}
