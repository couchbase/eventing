package pointconnection

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

var (
	userName = "Administrator"
	password = "password"
)

type serverResponse struct {
	Method string              `json:"method"`
	Query  map[string][]string `json:"query"`
	Header map[string][]string `json:"header"`
	Body   []byte              `json:"body"`
}

func extractBase64Pair(s string) (user, extra string, err error) {
	decoded, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return
	}
	idx := bytes.IndexByte(decoded, ':')
	if idx < 0 {
		err = fmt.Errorf("malformed header")
		return
	}
	user = string(decoded[0:idx])
	extra = string(decoded[(idx + 1):])
	return
}

// ExtractCreds extracts Basic auth creds from request.
func ExtractCreds(req *http.Request) (user string, pwd string, err error) {
	auth := req.Header.Get("Authorization")
	if auth == "" {
		return "", "", nil
	}

	basicPrefix := "Basic "
	if !strings.HasPrefix(auth, basicPrefix) {
		err = fmt.Errorf("non-basic auth is not supported")
		return
	}
	return extractBase64Pair(auth[len(basicPrefix):])
}

func cloneClientRequest(r *http.Request) serverResponse {
	s := serverResponse{}
	s.Method = r.Method
	s.Query = r.URL.Query()
	s.Header = r.Header

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return s
	}

	s.Body = data
	return s
}

func echoRequest(w http.ResponseWriter, r *http.Request) {
	user, pwd, err := ExtractCreds(r)
	if err != nil || user != userName || pwd != password {
		// TODO: send authentication error
		return
	}

	s := cloneClientRequest(r)
	data, err := json.Marshal(s)
	if err != nil {
		log.Printf("Error in marshalling: %v", err)
	}
	w.Write(data)
}

type httpHandler func(w http.ResponseWriter, r *http.Request)

func timeoutHandlerFunction(timeout time.Duration,
	maxParallelConnection int) func(w http.ResponseWriter, r *http.Request) {
	numRequest := int32(0)
	return func(w http.ResponseWriter, r *http.Request) {
		s := serverResponse{}
		if atomic.AddInt32(&numRequest, 1) > int32(maxParallelConnection) {
			atomic.AddInt32(&numRequest, -1)
			s.Body = []byte(fmt.Sprintf("More than MaxParallel request is made: %d", maxParallelConnection))
			data, _ := json.Marshal(s)
			w.Write(data)
			return
		}

		time.Sleep(timeout)
		atomic.AddInt32(&numRequest, -1)
		s.Body = []byte("Done sleeping")
		data, _ := json.Marshal(s)
		w.Write(data)
		return
	}
}

func statusHandlerFunction(statusCode int) func(w http.ResponseWriter, r *http.Request) {
	switch statusCode {
	case http.StatusTooManyRequests:
		oldRequest := time.Now()
		timeDiff := 0
		maxNumRequest := 6
		return func(w http.ResponseWriter, r *http.Request) {
			if float64(timeDiff) > time.Now().Sub(oldRequest).Seconds() || maxNumRequest == 0 {
				w.Write([]byte(fmt.Sprintf("After asked was: %v but request is made early", timeDiff)))
				return
			}

			oldRequest = time.Now()
			timeDiff++
			maxNumRequest--
			// ask to retry after
			w.Header().Set("Retry-After", strconv.Itoa(timeDiff))
			w.WriteHeader(statusCode)
			return
		}
	default:
		return func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(statusCode)
			return
		}
	}
}

func streamingHandlerFunction() func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		i := 0
		var err error
		for err == nil {
			_, err = w.Write([]byte(strconv.Itoa(i)))
			i++
			_, err = w.Write([]byte("\n"))
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func noOpHTTPHandler() func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("\n"))
	}
}
