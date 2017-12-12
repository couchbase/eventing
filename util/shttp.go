package util

import (
	"github.com/couchbase/cbauth"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Client struct {
	http.Client
}

var DefaultClient = &Client{}

func NewClient(timeout time.Duration) *Client {
	return &Client{http.Client{Timeout: timeout}}
}

func (c *Client) Do(req *http.Request) (*http.Response, error) {
	cbauth.SetRequestAuthVia(req, nil)
	return c.Client.Do(req)
}

func (c *Client) Get(url string) (resp *http.Response, err error) {
	req, err := NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	cbauth.SetRequestAuthVia(req, nil)
	return c.Client.Do(req)
}

func (c *Client) Head(url string) (resp *http.Response, err error) {
	req, err := NewRequest("HEAD", url, nil)
	if err != nil {
		return nil, err
	}
	cbauth.SetRequestAuthVia(req, nil)
	return c.Client.Do(req)
}

func (c *Client) Post(url string, contentType string, body io.Reader) (resp *http.Response, err error) {
	req, err := NewRequest("POST", url, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", contentType)
	cbauth.SetRequestAuthVia(req, nil)
	return c.Client.Do(req)
}

func (c *Client) PostForm(url string, data url.Values) (resp *http.Response, err error) {
	return c.Post(url, "application/x-www-form-urlencoded", strings.NewReader(data.Encode()))
}

func Do(req *http.Request) (*http.Response, error) {
	return DefaultClient.Do(req)
}

func Get(url string) (resp *http.Response, err error) {
	return DefaultClient.Get(url)
}

func Head(url string) (resp *http.Response, err error) {
	return DefaultClient.Head(url)
}

func Post(url string, contentType string, body io.Reader) (resp *http.Response, err error) {
	return DefaultClient.Post(url, contentType, body)
}

func PostForm(url string, data url.Values) (resp *http.Response, err error) {
	return DefaultClient.PostForm(url, data)
}

func NewRequest(method, url string, body io.Reader) (*http.Request, error) {
	return http.NewRequest(method, url, body)
}
