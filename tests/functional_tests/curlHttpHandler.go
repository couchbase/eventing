package eventing

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"

	"github.com/couchbase/eventing/tests/auth"
)

func setCookiesIfAllowed(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Accept-Cookies") == "true" {
		cookie := &http.Cookie{
			Name:  "cookie-key",
			Value: "cookie-value",
			Path:  "/",
		}
		http.SetCookie(w, cookie)
	}
}

func headHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "HEAD" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if !handleAuthorization(w, r) {
		return
	}
}

func postOrPutHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	if !(r.Method == "POST" || r.Method == "PUT") {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	setCookiesIfAllowed(w, r)

	if !handleAuthorization(w, r) {
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	var verification Verification

	if r.Header.Get("Accept-Cookies") == "true" {
		verification.HandleCookie(r)
	}

	switch r.Header.Get("Content-Type") {
	case "application/octet-stream":
		verification.HandleBinary(body)

	case "application/x-www-form-urlencoded":
		verification.HandleForm(body)

	case "application/json":
		verification.HandleJSON(body)

	case "text/plain":
		verification.HandleText(body)
	}

	response, err := json.Marshal(verification)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	fmt.Fprintf(w, "%s", string(response))
}

func emptyHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" || r.Method == "DELETE" {
		switch r.Header.Get("Accept") {
		case "application/json":
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
		case "application/x-www-form-urlencoded":
			w.Header().Set("Content-Type", "application/x-www-form-urlencoded; charset=utf-8")
		case "image/png":
			w.Header().Set("Content-Type", "image/png")
		case "text/plain":
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		default:
			return
		}
		return
	}

	if r.Method == "POST" || r.Method == "PUT" {
		return
	}
	w.WriteHeader(http.StatusMethodNotAllowed)
}

func largeHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" || r.Method == "PUT" {
		expectedSize, err := strconv.Atoi(r.Header.Get("Body-Size"))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		actualSize := len(body)
		if actualSize != expectedSize {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		var data map[string]string
		err = json.Unmarshal(body, &data)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		// Validate that the data received has no glitches
		for _, c := range data["key"] {
			if c != '1' {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
		}
		return
	}

	if r.Method == "GET" {
		value := ""
		size := 5 * 1024
		for i := 0; i < size; i++ {
			value += "1"
		}

		data := make(map[string]string)
		data["key"] = value
		body, err := json.Marshal(data)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Value-Size", strconv.Itoa(size))
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		fmt.Fprintf(w, "%s", string(body))
		return
	}
	w.WriteHeader(http.StatusMethodNotAllowed)
}

func handleAuthorization(w http.ResponseWriter, r *http.Request) bool {
	basicAuth := &auth.BasicAuth{
		Username: "Administrator",
		Password: "asdasd",
	}

	digestAuth := &auth.DigestAuth{
		Password: "asdasd",
		Method:   r.Method,
		Realm:    "eventing",
		Qop:      "auth",
		Nonce:    "dcd98b7102dd2f0e8b11d0f600bfb0c093",
		Opaque:   "5ccc069c403ebaf9f0171e9517f40e41",
	}

	authorization := r.Header.Get("Authorization")

	switch r.URL.Path {
	case "/put/auth":
		fallthrough
	case "/post/auth":
		fallthrough
	case "/head/auth":
		fallthrough
	case "/delete/auth":
		fallthrough
	case "/get/auth":
		authRequest, err := auth.NewBasicAuth(authorization)
		if err != nil || !basicAuth.Validate(authRequest) {
			w.WriteHeader(http.StatusUnauthorized)
			return false
		}

	case "/put/auth/digest":
		fallthrough
	case "/post/auth/digest":
		fallthrough
	case "/head/auth/digest":
		fallthrough
	case "/delete/auth/digest":
		fallthrough
	case "/get/auth/digest":
		if len(authorization) == 0 ||
			!digestAuth.Validate(auth.NewDigestRequest(authorization)) {
			w.Header().Set("WWW-Authenticate", digestAuth.GetHeader())
			w.WriteHeader(http.StatusUnauthorized)
			return false
		}
	}
	return true
}

func getOrDeleteHandler(w http.ResponseWriter, r *http.Request) {
	if !(r.Method == "GET" || r.Method == "DELETE") {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	setCookiesIfAllowed(w, r)

	if !handleAuthorization(w, r) {
		return
	}

	if r.URL.Path == "/get/url-params" {
		if r.URL.RawQuery != "1=2&key=value&array=%5B%22yes%22%2C%22this%22%2C%22is%22%2C%22an%22%2C%22array%22%5D" {
			w.WriteHeader(http.StatusInternalServerError)
		}
		return
	}

	switch r.Header.Get("Accept") {
	case "application/json":
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		fmt.Fprintf(w, "%v", `{"key":"here comes some value as application/json"}`)

	// This is not a standard type
	case "application/malformed-json":
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		fmt.Fprintf(w, "%v", `{"key":"here comes some value as application/json and is malformed`)

	case "application/x-www-form-urlencoded":
		w.Header().Set("Content-Type", "application/x-www-form-urlencoded; charset=utf-8")
		k1 := "some-key"
		v1 := "some-value"
		k2 := "another-key"
		v2 := "another-value"

		data := url.QueryEscape(k1) + "=" + url.QueryEscape(v1) + "&" +
			url.QueryEscape(k2) + "=" + url.QueryEscape(v2)
		fmt.Fprintf(w, "%v", data)

	case "image/png":
		w.Header().Set("Content-Type", "image/png")
		data := []uint8{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 42, 0, 0, 0}
		w.Write(data)

	case "text/plain":
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprint(w, "here comes some value as text/plain")

	default:
		w.Header().Set("Content-Type", "unknown content type")
		fmt.Fprint(w, "here comes some body of unknown content type")
	}
}
