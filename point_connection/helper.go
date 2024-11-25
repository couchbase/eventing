package pointconnection

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// DefaultCallback used for terminating further processing
// of the request and end the request downstream
func DefaultCallback(_ *Response) WhatNext {
	return Stop
}

// DefaultBackoff used as default backoff if none is provided in the backoff
// Algorithm: If server asks for backoff it uses server provided
// seconds to retry else backoff every 1second
func DefaultBackoff(resp *Response) time.Duration {
	backoff := time.Second
	if resp == nil {
		return backoff
	}

	if s, ok := resp.Header["Retry-After"]; ok {
		if sleep, err := strconv.ParseInt(s[0], 10, 64); err == nil {
			backoff = time.Second * time.Duration(sleep)
		}
	}

	return backoff
}

func resetResponse(resp *Response) {
	resp.StatusCode = 0
	resp.Header = nil
	resp.Body = resp.Body[:0]
	resp.Retry = false
	resp.Success = false
	resp.Err = nil
}

func populateResponse(httpResponse *http.Response, err error, resp *Response) {
	resetResponse(resp)
	resp.Err = err
	if err != nil {
		return
	}

	resp.StatusCode = httpResponse.StatusCode
	resp.Header = httpResponse.Header

	// check for status handle
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		resp.Success = true
		resp.Retry = true
		return
	}

	data, err := io.ReadAll(httpResponse.Body)
	if err != nil {
		resp.Err = err
	} else {
		resp.Err = fmt.Errorf("%s", data)
	}

	if isRetriable(resp.StatusCode) {
		resp.Retry = true
	}

	return
}

var (
	retriableStatusCode = map[int]struct{}{
		http.StatusTooManyRequests:     {},
		http.StatusFound:               {},
		http.StatusTemporaryRedirect:   {},
		http.StatusRequestTimeout:      {},
		http.StatusTooEarly:            {},
		http.StatusInternalServerError: {},
		http.StatusBadGateway:          {},
		http.StatusServiceUnavailable:  {},
		http.StatusGatewayTimeout:      {},
		http.StatusInsufficientStorage: {},
	}
)

func isRetriable(statusCode int) bool {
	_, ok := retriableStatusCode[statusCode]
	return ok
}

type tokenMap struct {
	sync.RWMutex
	tokenMap  map[Token]*internalRequest
	lastToken Token
}

func newTokenMap() *tokenMap {
	return &tokenMap{
		tokenMap:  make(map[Token]*internalRequest),
		lastToken: Token(0),
	}
}

func (t *tokenMap) createNewToken(r *internalRequest) Token {
	t.Lock()
	defer t.Unlock()

	lToken := t.lastToken
	t.tokenMap[lToken] = r
	t.lastToken++
	return lToken
}

func (t *tokenMap) deleteToken(id Token) *internalRequest {
	t.Lock()
	defer t.Unlock()

	r := t.tokenMap[id]
	delete(t.tokenMap, id)
	return r
}
