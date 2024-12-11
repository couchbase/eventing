package pointconnection

import (
	"context"
	"crypto/tls"
	"errors"
	"net/http"
	"time"
)

// WhatNext will dictate what to do after calling a
// callback provided during request
type WhatNext int8

// Callback should give one of this response
const (
	// Wait for more bytes from the connection
	Continue WhatNext = iota

	// Stop the request
	Stop

	// Retry the request. Callback can be called with
	// duplicate response.
	RetryRequest
)

var (
	// ErrEndOfConnection notifies that connection is terminated by server
	// or network issue
	// If this is provided in the callback
	// client should give back Stop/RetryRequest
	ErrEndOfConnection = errors.New("end of connection")
)

// ConnSettings is provided when creating a PointConnection request
// This will dictate the connection semantics
type ConnSettings struct {
	// Maximum number of connections per each host which may be established.
	// This will determine the number of parallel request per host
	// DefaultMaxConnsPerHost is used if not set.
	MaxConnsPerHost int

	// How long the connection should be kept open
	MaxIdleConnection int

	// RootCertificatePath specifies from where certificate needs to be read
	// If empty then it will accept any certificate
	// Maybe a function is better
	RootCertificatePath string

	// ClientPrivateKey specifies the private key for a client
	ClientPrivateKey string

	// ClientCertificatePath specifies from where can we find the certificate path
	ClientCertificatePath string

	// ClientCertificate is the certificate which can be used by the http client
	// when client certificate authentication is mandatory
	ClientCertificate *tls.Certificate

	// TlsConfig will give the config that client wants to use
	// If provided other tls realted config will be ignored
	TlsConfig *tls.Config
}

// Response is the response to the provided response
type Response struct {
	// Status code for the response
	StatusCode int

	// Header sent by the server
	Header map[string][]string

	// Body of the function
	// It can be chucked. It will be reset after
	// callback is called.
	// Callback should store it whatever way they want
	Body []byte

	// Hint that retry might be successfull based on the
	// error and response header
	// This shouldn't be used when reading the body
	// When Success is false
	Retry bool

	// Indicate that request is successfully made
	// If its false check retry for deciding whether to retry or exit
	Success bool

	// Error if there is any error with connection
	// It can be temporary in which case Retry hint will
	// tell whether retry helps or not
	Err error
}

// ResponseCallback will be called when it receives the response.
// Its the callbacks responsibility to store the bytes if
// chunked response is received. It shoudl return what the
// caller must do. For ErrEndOFConnection return value should be
// either Stop or RetryRequest
type ResponseCallback func(*Response) WhatNext

// BackOffFunction shoudl return how much seconds to backoff before retrying
type BackOffFunction func(*Response) time.Duration

const (
	// GET http method
	GET = http.MethodGet

	// PUT http method
	PUT = http.MethodPut

	// POST http method
	POST = http.MethodPost

	// DELETE http method
	DELETE = http.MethodDelete
)

// AuthFunction should return valid user name and password
// This is used to authenticate with the server
type AuthFunction func(req *http.Request)

// Request that needs to be done by the point connection instance
type Request struct {
	URL    string              // request and path
	Query  map[string][]string // Query
	Method string
	Header map[string][]string // Headers to be included with this request
	Body   []byte              // Body to be included with this request

	// If no delimiter is set then it will read till end of connection
	// Should be careful to make streaming endpoint
	Delim    byte
	Callback ResponseCallback

	// Used to get authentication details for this request
	GetAuth AuthFunction

	// By default 0 retries. Request will be made only once
	MaxTempFailRetry int
	Timeout          time.Duration

	// How  much time to wait before retrying the request
	// by default no wait
	BackOffMilliSeconds BackOffFunction
}

type Token int

// PointConnection is the interface to make a point request to a host
type PointConnection interface {
	// Send the request and all the response comes in the callback
	// It can error out if something problematic is passed into req
	// server side errors will be present in callback.
	// error retruns when there is a issue in creating a request
	AsyncSend(req *Request) (Token, error)

	// Same as AsyncSend
	// Accepts the context. Processing stops when this context is cancelled
	AsynSendWithContext(ctx context.Context, req *Request) (Token, error)

	// Its a blocking call till all the response is received from the
	// server. Response contains issue with request
	// error is if there is some error in creating the request
	// If timeout is not specified it can block forever
	SyncSend(req *Request) (*Response, error)

	// Same as SyncSend.
	// Accepts the context. Processing stops when this context is cancelled
	SyncSendWithContext(ctx context.Context, req *Request) (*Response, error)

	// Stop the request
	// After return from this function there won't be any callback call
	// associated with this request
	// This function shouldn't be called in the callback
	Stop(t Token)
}
