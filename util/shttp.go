package util

import (
	"crypto/tls"
	"crypto/x509"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/logging"
)

type Client struct {
	http.Client
}

var DefaultClient = &Client{}

func NewClient(timeout time.Duration) *Client {
	return &Client{http.Client{Timeout: timeout}}
}

func NewTLSClient(timeout time.Duration, config *common.SecuritySetting) *Client {
	pemFile := config.CertFile
	if len(config.CAFile) > 0 {
		pemFile = config.CAFile
	}
	cert, err := ioutil.ReadFile(pemFile)
	if err != nil {
		return &Client{http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					RootCAs: config.RootCAs,
				},
			},
		}}
	}
	caPool := x509.NewCertPool()
	caPool.AppendCertsFromPEM(cert)

	return &Client{http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs: caPool,
			},
		},
	}}
}

func (c *Client) Do(req *http.Request) (*http.Response, error) {
	cbauth.SetRequestAuthVia(req, nil)
	return c.Client.Do(req)
}

func (c *Client) Get(url string) (resp *http.Response, err error) {
	logPrefix := "Client::Get"

	req, err := NewRequest("GET", url, nil)
	if err != nil {
		logging.Errorf("%s URL: %rs Encountered err: %v", logPrefix, url, err)
		return nil, err
	}

	err = cbauth.SetRequestAuthVia(req, nil)
	if err != nil {
		logging.Errorf("%s URL: %rs Failed to set auth params, err: %v", logPrefix, url, err)
		return nil, err
	}

	return c.Client.Do(req)
}

func (c *Client) Head(url string) (resp *http.Response, err error) {
	logPrefix := "Client::Head"

	req, err := NewRequest("HEAD", url, nil)
	if err != nil {
		logging.Errorf("%s URL: %rs Encountered err: %v", logPrefix, url, err)
		return nil, err
	}

	err = cbauth.SetRequestAuthVia(req, nil)
	if err != nil {
		logging.Errorf("%s URL: %rs Failed to set auth params, err: %v", logPrefix, url, err)
		return nil, err
	}

	return c.Client.Do(req)
}

func (c *Client) Delete(url string) (resp *http.Response, err error) {
	logPrefix := "Client::Delete"

	req, err := NewRequest("DELETE", url, nil)
	if err != nil {
		logging.Errorf("%s URL: %rs Encountered err: %v", logPrefix, url, err)
		return nil, err
	}

	err = cbauth.SetRequestAuthVia(req, nil)
	if err != nil {
		logging.Errorf("%s URL: %rs Failed to set auth params, err: %v", logPrefix, url, err)
		return nil, err
	}

	return c.Client.Do(req)
}

func (c *Client) Post(url string, contentType string, body io.Reader) (resp *http.Response, err error) {
	logPrefix := "Client::Post"

	req, err := NewRequest("POST", url, body)
	if err != nil {
		logging.Errorf("%s URL: %rs Encountered err: %v", logPrefix, url, err)
		return nil, err
	}
	req.Header.Set("Content-Type", contentType)

	err = cbauth.SetRequestAuthVia(req, nil)
	if err != nil {
		logging.Errorf("%s URL: %rs Failed to set auth params, err: %v", logPrefix, url, err)
		return nil, err
	}

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
