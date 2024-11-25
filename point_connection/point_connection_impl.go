package pointconnection

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"
)

type pointConnection struct {
	conn *http.Client

	tokenHandler *tokenMap
}

// Maybe this can be made generic
type callbackObject struct {
	sync.Mutex
	callback ResponseCallback
}

func newCallbackObject(newCallback ResponseCallback) *callbackObject {
	return &callbackObject{callback: newCallback}
}

func (call *callbackObject) call(response *Response) WhatNext {
	call.Lock()
	defer call.Unlock()
	if call.callback == nil {
		return Stop
	}

	return call.callback(response)
}

func (call *callbackObject) replace(newCallback ResponseCallback) {
	call.Lock()
	defer call.Unlock()

	call.callback = newCallback
}

type internalRequest struct {
	httpReq *http.Request
	res     *Response

	backOffMilliSeconds BackOffFunction
	callback            *callbackObject
	cancelFunc          context.CancelFunc
	GetAuth             AuthFunction

	maxTempFailRetry int
	timeout          time.Duration

	delim byte

	setupDone chan Token
}

// NewPointConnection creates a new PointConnection object with
// provided settings. This can be used to make a request either
// sync or async request
// PointConnection is thread safe and should be reused
// Currently it will return error if there is some issue with the tls config
func NewPointConnection(settings *ConnSettings) (PointConnection, error) {
	p := &pointConnection{}
	conn, err := settings.getConnFromSetting()
	if err != nil {
		return nil, err
	}

	p.conn = conn
	p.tokenHandler = newTokenMap()
	return p, nil
}

func (p *pointConnection) AsyncSend(req *Request) (Token, error) {
	return p.AsynSendWithContext(context.Background(), req)
}

func (p *pointConnection) AsynSendWithContext(ctx context.Context, req *Request) (Token, error) {
	r, err := req.createHTTPRequest()
	if err != nil {
		return Token(-1), err
	}

	go p.send(ctx, r)
	tokenID := <-r.setupDone
	return tokenID, nil
}

func (p *pointConnection) SyncSend(req *Request) (*Response, error) {
	return p.SyncSendWithContext(context.Background(), req)
}

func (p *pointConnection) SyncSendWithContext(ctx context.Context, req *Request) (*Response, error) {
	r, err := req.createHTTPRequest()
	if err != nil {
		return nil, err
	}

	r.callback.replace(DefaultCallback)
	p.send(ctx, r)

	// No need for this but just to clean it up
	<-r.setupDone

	return r.res, nil
}

// This will call cancelFunc of context which internally triggers
// cancel to its child Context and exit from it
// Replacing callback with nil ensures that there won't be
// any further call to callback
func (p *pointConnection) Stop(id Token) {
	req := p.tokenHandler.deleteToken(id)
	if req == nil {
		return
	}

	req.callback.replace(nil)
	req.cancelFunc()
}

func (p *pointConnection) send(ctx context.Context, req *internalRequest) {
	next := RetryRequest
	retry := req.maxTempFailRetry
	// Always cancel this function in defer since it can lead to memory leak
	ctx, cancelFunc := context.WithCancel(ctx)
	req.cancelFunc = cancelFunc

	id := p.tokenHandler.createNewToken(req)

	defer func() {
		p.tokenHandler.deleteToken(id)
		cancelFunc()
	}()

	req.setupDone <- id

	for {
		next = p.sendOnce(ctx, req)
		if next == Stop {
			return
		}

		if retry > 0 {
			retry--
		}
		switch {
		case retry != 0:
			next = RetryRequest
		default:
			req.callback.call(req.res)
			return
		}
		backoff := req.backOffMilliSeconds(req.res)
		time.Sleep(backoff)
	}
}

func (p *pointConnection) sendOnce(ctx context.Context, request *internalRequest) (next WhatNext) {
	next = RetryRequest
	httpRequest := request.prepareRequest()
	if request.timeout != 0 {
		ctx, cancelFunc := context.WithTimeout(ctx, request.timeout)
		defer cancelFunc()
		httpRequest = httpRequest.WithContext(ctx)
	}

	resp, err := p.conn.Do(httpRequest)
	populateResponse(resp, err, request.res)
	if err == nil {
		defer resp.Body.Close()
	}

	if !request.res.Success {
		if request.res.Retry {
			next = Continue
			return
		}
		next = request.callback.call(request.res)
		return
	}
	reader := bufio.NewReader(resp.Body)

	next = Continue
	for next == Continue {
		request.res.Body, err = reader.ReadBytes(request.delim)
		if err == io.EOF {
			err = ErrEndOfConnection
		}

		request.res.Err = err
		// Allow client to handle the error returned
		// If EOF client should return either Stop or RetryRequest
		next = request.callback.call(request.res)
	}
	return
}

// Internal functions for ConnSettings struct
func (s *ConnSettings) getConnFromSetting() (*http.Client, error) {
	transport := &http.Transport{}
	if s.MaxConnsPerHost != 0 {
		transport.MaxConnsPerHost = s.MaxConnsPerHost
	}

	if s.MaxIdleConnection != 0 {
		transport.MaxIdleConns = s.MaxIdleConnection
	}

	tlsConfig, err := s.getTlsConfig()
	if err != nil {
		return nil, err
	}
	transport.TLSClientConfig = tlsConfig

	client := &http.Client{
		Transport: transport,
	}

	return client, nil
}

// getTlsConfig returns tls.config from the connection settings
func (s *ConnSettings) getTlsConfig() (config *tls.Config, err error) {
	if s.TlsConfig != nil {
		return s.TlsConfig.Clone(), nil
	}

	config = &tls.Config{}
	config.RootCAs, config.InsecureSkipVerify, err = s.getRootCAs()
	if err != nil {
		return
	}

	config.Certificates, err = s.getClientCertificates()
	if err != nil {
		return
	}

	return
}

func (s *ConnSettings) getRootCAs() (*x509.CertPool, bool, error) {
	if len(s.RootCertificatePath) == 0 {
		return nil, true, nil
	}

	rootCAPool := x509.NewCertPool()
	rootCA, err := os.ReadFile(s.RootCertificatePath)
	if err != nil {
		return nil, true, fmt.Errorf("error reading rootcertificate: %s err: %v", s.RootCertificatePath, err)
	}

	ok := rootCAPool.AppendCertsFromPEM(rootCA)
	if !ok {
		return nil, true, fmt.Errorf("no Pem encoded certificates found")
	}
	return rootCAPool, false, nil
}

func (s *ConnSettings) getClientCertificates() ([]tls.Certificate, error) {
	// If both are nil then there won't be any client certificate provided by user
	// If any one of them is provided fall through. In that case tls.Load should
	// give error
	if len(s.ClientPrivateKey) == 0 && len(s.ClientCertificatePath) == 0 {
		return nil, nil
	}

	cert, err := tls.LoadX509KeyPair(s.ClientCertificatePath, s.ClientPrivateKey)
	if err != nil {
		return nil, fmt.Errorf("error loading x509key pair path: %s err: %v", s.ClientCertificatePath, err)
	}

	return []tls.Certificate{cert}, nil
}

// Internal functions for Request struct

// Clone will copy all the fields in a
func (r *Request) clone() *Request {
	clonedRequest := *r

	clonedRequest.Query = make(map[string][]string)
	for key, values := range r.Query {
		attributes := make([]string, len(values))
		copy(attributes, values)
		clonedRequest.Query[key] = attributes
	}

	clonedRequest.Header = make(map[string][]string)
	for key, values := range r.Header {
		attributes := make([]string, len(values))
		copy(attributes, values)
		clonedRequest.Header[key] = attributes
	}

	copy(clonedRequest.Body, r.Body)
	return &clonedRequest
}

func (r *Request) createHTTPRequest() (*internalRequest, error) {
	httpReq, err := http.NewRequest(r.Method, r.URL, bytes.NewReader(r.Body))
	if err != nil {
		return nil, err
	}

	// Add headers to request
	for k, values := range r.Header {
		for _, val := range values {
			httpReq.Header.Add(k, val)
		}
	}

	// Add query parameters to request
	q := httpReq.URL.Query()
	for k, values := range r.Query {
		for _, val := range values {
			q.Add(k, val)
		}
	}
	httpReq.URL.RawQuery = q.Encode()

	backOffMilliSeconds := DefaultBackoff
	if r.BackOffMilliSeconds != nil {
		backOffMilliSeconds = r.BackOffMilliSeconds
	}

	internal := &internalRequest{
		// one token will be present for each request
		setupDone:           make(chan Token, 1),
		httpReq:             httpReq,
		res:                 &Response{},
		backOffMilliSeconds: backOffMilliSeconds,
		callback:            newCallbackObject(r.Callback),
		delim:               r.Delim,
		timeout:             r.Timeout,
		maxTempFailRetry:    r.MaxTempFailRetry,
		GetAuth:             r.GetAuth,
	}

	return internal, nil
}

func (r *internalRequest) prepareRequest() *http.Request {
	if r.GetAuth != nil {
		r.GetAuth(r.httpReq)
	}
	return r.httpReq
}
