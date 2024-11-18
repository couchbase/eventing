package pointconnection

import (
	"crypto/tls"
	"encoding/json"
	"fmt"

	// "log"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func authHandler(req *http.Request) {
	req.SetBasicAuth(userName, password)
}

func TestRequestClone(t *testing.T) {
	callback := func(next WhatNext) ResponseCallback {
		return func(_ *Response) WhatNext {
			return next
		}
	}

	r := &Request{
		URL:      "new",
		Query:    map[string][]string{"query": {"unchanged"}},
		Header:   map[string][]string{"header": {"unchanged"}},
		Body:     []byte("unchanged"),
		Callback: callback(Stop),
	}

	cloned := r.clone()
	cloned.Query["query"][0] = "changed"
	cloned.Header["query2"] = []string{"newQuery"}
	if _, ok := r.Query["query2"]; ok || r.Query["query"][0] != "unchanged" {
		t.Fatalf("Query field changed due to change in cloned request")
	}

	cloned.Header["header"][0] = "changed"
	cloned.Header["header2"] = []string{"newHeader"}
	if _, ok := r.Header["header2"]; ok || r.Header["header"][0] != "unchanged" {
		t.Fatalf("Header field changed due to change in cloned request")
	}

	cloned.Body = []byte("changed")
	if string(r.Body) != "unchanged" {
		t.Fatalf("Body field changed due to change in cloned request")
	}

	cloned.Callback = callback(Continue)
	if r.Callback(nil) != Stop {
		t.Fatalf("Callback field changed due to change in cloned request")
	}
}

func TestErrorInputCondition(t *testing.T) {
	setting := &ConnSettings{MaxIdleConnection: 100}

	setting.RootCertificatePath = "servercertfiles/pkey.key"
	_, err := NewPointConnection(setting)
	if err == nil {
		t.Fatalf("Expected root certificate error got nil")
	}

	setting.RootCertificatePath = "servercertfiles/nothing.pem"
	_, err = NewPointConnection(setting)
	if err == nil {
		t.Fatalf("Expected root certificate error got nil")
	}

	setting.RootCertificatePath = ""
	setting.ClientPrivateKey = "servercertfiles/pkey.key"
	_, err = NewPointConnection(setting)
	if err == nil {
		t.Fatalf("Expected error since there is no client certificate provided")
	}

	setting.ClientCertificatePath = "servercertfiles/pkey.key"
	_, err = NewPointConnection(setting)
	if err == nil {
		t.Fatalf("Expected error due to invalid client certificate provided")
	}
}

// Test for all methods of sync send and Post Body
func TestSyncSendDifferentMethod(t *testing.T) {
	p, err := NewPointConnection(&ConnSettings{})
	if err != nil {
		t.Fatalf("Error while creating PointConnection: %v, expected nil", err)
	}

	ts := httptest.NewServer(http.HandlerFunc(echoRequest))
	defer ts.Close()

	req := &Request{
		URL:     ts.URL,
		GetAuth: authHandler,
	}

	reqMethod := [4]string{GET, POST, DELETE, PUT}

	for _, method := range reqMethod {
		req.Method = method
		if method == POST {
			req.Body = []byte(POST)
		}

		res, err := p.SyncSend(req)
		if err != nil {
			t.Errorf("Got error from SyncSend %v Expected nil", err)
		}

		s := serverResponse{}
		err = json.Unmarshal(res.Body, &s)
		if err != nil {
			t.Errorf("Not able to unmarshal response: %v", res.Body)
		}

		if s.Method != method || string(req.Body) != string(s.Body) {
			t.Errorf("Unexpected method sent: %v method: %v", s, method)
		}
	}
}

// Test for async send of different methods with query and header integrity from client
func TestAsyncSendDifferentMethod(t *testing.T) {
	p, err := NewPointConnection(&ConnSettings{})
	if err != nil {
		t.Fatalf("Error while creating PointConnection: %v, expected nil", err)
	}

	ts := httptest.NewServer(http.HandlerFunc(echoRequest))
	defer ts.Close()

	req := &Request{
		URL:     ts.URL,
		GetAuth: authHandler,
		Header:  map[string][]string{"Check-Header": {"application/json"}},
		Query:   map[string][]string{"typeRequest": {""}},
	}

	callback := func(reqType string, ch chan<- error) ResponseCallback {
		return func(res *Response) (stop WhatNext) {
			stop = Stop
			if res.Err != nil {
				ch <- res.Err
				return
			}

			s := serverResponse{}
			err := json.Unmarshal(res.Body, &s)
			if err != nil {
				ch <- fmt.Errorf("unable to unmarshal the response: %v", err)
				return
			}

			if s.Header["Check-Header"][0] != "application/json" {
				ch <- fmt.Errorf("Header is changed: %v", s.Header)
				return
			}

			if s.Query["typeRequest"][0] != reqType {
				ch <- fmt.Errorf("Query is changed: %v", s.Query)
				return
			}

			ch <- nil
			return
		}
	}

	reqMethod := [4]string{GET, POST, DELETE, PUT}
	countChannel := make(chan error, len(reqMethod))
	for _, method := range reqMethod {
		go func(method string) {
			tmpReq := req.clone()
			tmpReq.Method = method
			tmpReq.Query["typeRequest"][0] = method
			tmpReq.Callback = callback(method, countChannel)
			_, err := p.AsyncSend(tmpReq)
			if err != nil {
				t.Errorf("Got error from AsyncSend %v Expected nil", err)
			}
		}(method)

	}

	for i := 0; i < len(reqMethod); i++ {
		err := <-countChannel
		if err != nil && err != ErrEndOfConnection {
			t.Fatalf("Error response in callback: %v", err)
		}
	}
}

// Test for timeout and maximum parallel connection per host
func TestConnectionSettingsAndTimeout(t *testing.T) {
	timeout := time.Duration(3 * time.Second)
	maxConnection := 3

	timeoutHandler := timeoutHandlerFunction(time.Duration(timeout*time.Duration(2)), maxConnection)
	ts := httptest.NewServer(http.HandlerFunc(timeoutHandler))
	defer ts.Close()

	settings := &ConnSettings{
		MaxConnsPerHost:   maxConnection,
		MaxIdleConnection: 0,
	}

	num := runtime.NumGoroutine()
	p, err := NewPointConnection(settings)
	if err != nil {
		t.Fatalf("Error while creating PointConnection: %v, expected nil", err)
	}

	ch := make(chan error, maxConnection+1)
	for i := 0; i < maxConnection+1; i++ {
		go func() {
			req := &Request{
				URL:     ts.URL,
				Timeout: timeout,
			}

			res, err := p.SyncSend(req)
			if err != nil {
				// Should get nil error
				ch <- err
				return
			}

			s := serverResponse{}
			json.Unmarshal(res.Body, &s)
			if res.Err != nil {
				ch <- fmt.Errorf("server sent response: %v", string(s.Body))
				return
			}
			ch <- nil
		}()
	}

	for i := 0; i < maxConnection+1; i++ {
		err := <-ch
		if err == nil {
			t.Fatalf("Got Success message for some connection")
			return
		}
	}

	// Wait for Sleep period on the server ends
	time.Sleep(timeout * 2)
	num2 := runtime.NumGoroutine()
	if num2 > num {
		pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
		t.Fatalf("Possible goroutime leak: %d end of test: %d", num, num2)
	}
}

func TestInvalidRequest(t *testing.T) {
	invalidReqHandler := statusHandlerFunction(http.StatusBadRequest)
	ts := httptest.NewServer(http.HandlerFunc(invalidReqHandler))
	defer ts.Close()

	p, err := NewPointConnection(&ConnSettings{MaxConnsPerHost: 1})
	if err != nil {
		t.Fatalf("Error while creating PointConnection: %v, expected nil", err)
	}

	req := &Request{
		URL: ts.URL,
	}

	doneRequest := make(chan struct{})
	for i := 0; i < 10; i++ {
		go func() {
			res, err := p.SyncSend(req)
			if err != nil {
				t.Fatalf("Error in creating request: %v. Expected nil", err)
			}

			if res.Success && res.StatusCode != http.StatusBadRequest && res.Err != nil {
				t.Fatalf("Request is made successfully. Error expected. %v status code: %v", string(res.Body), res.StatusCode)
			}
			doneRequest <- struct{}{}
		}()

		select {
		case <-time.After(5 * time.Second):
			t.Fatalf("Expected return from sync send")
		case <-doneRequest:
		}
	}
}

// Test retriable error code
func TestRetriableErrorCodeWithBackoff(t *testing.T) {
	retriableHandler := statusHandlerFunction(http.StatusTooManyRequests)
	ts := httptest.NewServer(http.HandlerFunc(retriableHandler))
	defer ts.Close()

	p, err := NewPointConnection(&ConnSettings{})
	if err != nil {
		t.Fatalf("Error while creating PointConnection: %v, expected nil", err)
	}

	req := &Request{
		URL:              ts.URL,
		MaxTempFailRetry: 5,
	}

	res, err := p.SyncSend(req)
	if err != nil {
		t.Fatalf("Error in creating request: %v. Expected nil", err)
	}

	if res.Success {
		t.Fatalf("Request is made successfully. Error expected. %v", string(res.Body))
	}
}

func TestStopRequest(t *testing.T) {
	num := runtime.NumGoroutine()

	streamingHandler := streamingHandlerFunction()
	ts := httptest.NewServer(http.HandlerFunc(streamingHandler))
	defer ts.Close()

	p, err := NewPointConnection(&ConnSettings{MaxIdleConnection: 0, MaxConnsPerHost: 0})
	if err != nil {
		t.Fatalf("Error while creating PointConnection: %v, expected nil", err)
	}

	req := &Request{
		URL:   ts.URL,
		Delim: byte('\n'),
	}

	called := int64(0)
	req.Callback = func(_ *Response) WhatNext {
		atomic.AddInt64(&called, int64(1))
		return Continue
	}

	tokenID, err := p.AsyncSend(req)
	if err != nil {
		t.Fatalf("Async Send shouldn't give error: %v, expected: nil", err)
	}

	// Wait for atleast 10 reply from server
	for atomic.LoadInt64(&called) < 10 {
		time.Sleep(time.Second)
	}
	p.Stop(tokenID)
	atomic.StoreInt64(&called, int64(-1))
	time.Sleep(10 * time.Second)
	if atomic.LoadInt64(&called) != int64(-1) {
		t.Fatalf("Called is not set to -1: Callback called after stop is being called")
	}

	ts.Close()
	num2 := runtime.NumGoroutine()
	if num2 > num {
		pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
		t.Fatalf("Possible go routine leak: %v old goroutine: %v", num2, num)
	}
}

// Test for streaming endpoint which include delimiter
func TestStreamingEndpoint(t *testing.T) {
	streamingHandler := streamingHandlerFunction()
	ts := httptest.NewServer(http.HandlerFunc(streamingHandler))
	defer ts.Close()

	p, err := NewPointConnection(&ConnSettings{})
	if err != nil {
		t.Fatalf("Error while creating PointConnection: %v, expected nil", err)
	}

	req := &Request{
		URL:   ts.URL,
		Delim: '\n',
	}

	num := 0
	returnCall := make(chan error)
	req.Callback = func(res *Response) WhatNext {
		resNum, err := strconv.Atoi(string(res.Body[:len(res.Body)-1]))
		if err != nil || resNum != num {
			returnCall <- fmt.Errorf("Err: %v, numExpected: %v got num: %v", err, num, resNum)
			return Stop
		}

		if num == 3 {
			returnCall <- nil
			return Stop
		}
		num++
		return Continue
	}

	_, err = p.AsyncSend(req)
	if err != nil {
		t.Fatalf("Async Send shouldn't give error: %v, expected: nil", err)
	}

	err = <-returnCall
	if err != nil {
		t.Fatalf("Expected nil return got some other return %v", err)
	}
}

// Test to breaking of connection in between receiving of the response
func TestStreamingEndpointWithCloseConnection(t *testing.T) {
	streamingHandler := streamingHandlerFunction()
	ts := httptest.NewServer(http.HandlerFunc(streamingHandler))
	defer ts.Close()

	p, err := NewPointConnection(&ConnSettings{})
	if err != nil {
		t.Fatalf("Error while creating PointConnection: %v, expected nil", err)
	}

	req := &Request{
		URL:   ts.URL,
		Delim: '\n',
	}

	returnCall := make(chan error, 1)
	req.Callback = func(res *Response) WhatNext {
		if res.Err != nil {
			returnCall <- res.Err
			return Stop
		}

		returnCall <- nil
		return Continue
	}

	_, err = p.AsyncSend(req)
	if err != nil {
		t.Fatalf("Async Send shouldn't give error: %v, expected: nil", err)
	}

	for i := 0; i < 3; i++ {
		<-returnCall
	}

	// Close the client connection after sending few message
	ts.CloseClientConnections()
	tick := time.NewTicker(5 * time.Second)
	for err == nil {
		select {
		case err = <-returnCall:
		case <-tick.C:
			t.Fatalf("Expected nil return got some other return %v", err)
		}
	}
}

type httpServer struct {
	addServerCertificate bool
	addClientCAs         bool
	tls                  bool
}

func (h *httpServer) newLocalHttpsServer(handler http.Handler) (*httptest.Server, error) {
	ts := httptest.NewUnstartedServer(handler)
	config := &tls.Config{}
	if h.addServerCertificate {
		cert, err := tls.LoadX509KeyPair("servercertfiles/chain.pem", "servercertfiles/pkey.key")
		if err != nil {
			return nil, err
		}
		config.Certificates = []tls.Certificate{cert}
	}

	if h.addClientCAs {
		s := &ConnSettings{RootCertificatePath: "servercertfiles/ca.pem"}
		certPool, _, err := s.getRootCAs()
		if err != nil {
			return nil, err
		}

		config.ClientCAs = certPool
		config.ClientAuth = tls.RequireAndVerifyClientCert
	}

	ts.TLS = config
	ts.StartTLS()
	return ts, nil
}

func TestTlsSyncSendDifferentMethod(t *testing.T) {
	setting := &ConnSettings{
		RootCertificatePath: "servercertfiles/ca.pem",
	}
	p, err := NewPointConnection(setting)
	if err != nil {
		t.Fatalf("Error while creating PointConnection: %v, expected nil", err)
	}

	h := httpServer{addServerCertificate: true}
	ts, err := h.newLocalHttpsServer(http.HandlerFunc(echoRequest))
	if err != nil {
		t.Fatalf("Error while creating https server: %v", err)
	}
	defer ts.Close()

	req := &Request{
		URL:     ts.URL,
		GetAuth: authHandler,
	}

	reqMethod := [3]string{GET, POST, DELETE}

	for _, method := range reqMethod {
		req.Method = method
		if method == POST {
			req.Body = []byte(POST)
		}

		res, err := p.SyncSend(req)
		if err != nil {
			t.Fatalf("Got error from SyncSend %v Expected nil", err)
		}

		s := serverResponse{}
		err = json.Unmarshal(res.Body, &s)
		if err != nil {
			t.Fatalf("Not able to unmarshal response: %v", res.Body)
		}

		if s.Method != method || string(req.Body) != string(s.Body) {
			t.Fatalf("Unexpected method sent: %v method: %v", s, method)
		}
	}
}

// Test for incorrect root ca on server side
func TestTlsSyncSendInvalidServerCertificate(t *testing.T) {
	setting := &ConnSettings{
		// Provided wrong ca cert file for client requests
		RootCertificatePath: "servercertfiles/diffCa/ca.pem",
	}
	p, err := NewPointConnection(setting)
	if err != nil {
		t.Fatalf("Error while creating PointConnection: %v, expected nil", err)
	}

	h := httpServer{addServerCertificate: true}
	ts, err := h.newLocalHttpsServer(http.HandlerFunc(echoRequest))
	if err != nil {
		t.Fatalf("Error while creating https server: %v", err)
	}
	defer ts.Close()

	req := &Request{
		URL:     ts.URL,
		GetAuth: authHandler,
	}

	res, err := p.SyncSend(req)
	if err != nil {
		t.Fatalf("Got error from SyncSend %v Expected nil", err)
	}

	if res.Err == nil {
		t.Fatalf("Expected error returns since client rootCAs doesn't verify server certificate")
	}
}

func TestTlsClientServerAuthentication(t *testing.T) {
	setting := &ConnSettings{
		RootCertificatePath:   "servercertfiles/ca.pem",
		ClientPrivateKey:      "servercertfiles/clientcertfiles/client.key",
		ClientCertificatePath: "servercertfiles/clientcertfiles/client.pem",
	}

	p, err := NewPointConnection(setting)
	if err != nil {
		t.Fatalf("Error while creating PointConnection: %v, expected nil", err)
	}

	h := httpServer{addServerCertificate: true, addClientCAs: true}
	ts, err := h.newLocalHttpsServer(http.HandlerFunc(echoRequest))
	if err != nil {
		t.Fatalf("Error while creating https server: %v", err)
	}
	defer ts.Close()

	req := &Request{
		URL:     ts.URL,
		GetAuth: authHandler,
	}

	reqMethod := [3]string{GET, POST, DELETE}
	for _, method := range reqMethod {
		req.Method = method
		if method == POST {
			req.Body = []byte(POST)
		}

		res, err := p.SyncSend(req)
		if err != nil {
			t.Errorf("Got error from SyncSend %v Expected nil", err)
		}

		s := serverResponse{}
		err = json.Unmarshal(res.Body, &s)
		if err != nil {
			t.Errorf("Not able to unmarshal response: %v", res.Body)
		}

		if s.Method != method || string(req.Body) != string(s.Body) {
			t.Errorf("Unexpected method sent: %v method: %v", s, method)
		}
	}
}

// Benchmark test
func BenchmarkSendWithoutTls(b *testing.B) {
	p, err := NewPointConnection(&ConnSettings{MaxConnsPerHost: 20})
	if err != nil {
		b.Fatalf("Got error return which shouldn't be")
	}

	noopHandler := noOpHTTPHandler()
	h := httpServer{addServerCertificate: false, addClientCAs: false}
	ts, err := h.newLocalHttpsServer(http.HandlerFunc(noopHandler))
	if err != nil {
		b.Fatalf("Error while creating https server: %v", err)
	}
	defer ts.Close()

	req := &Request{
		URL: ts.URL,
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		p.SyncSend(req)
	}
}
