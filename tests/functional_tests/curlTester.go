package eventing

import (
	"github.com/couchbase/eventing/common"
	"testing"
)

type curlTester struct {
	handler    string
	testName   string
	settings   *commonSettings
	testHandle *testing.T
}

func (c *curlTester) test() {
	response := createAndDeployFunction(c.handler, c.handler, c.settings)
	if response.err != nil {
		c.testHandle.Errorf("Unable to deploy Function %s, err : %v", c.handler, response.err)
		return
	}

	ops := opsType{count: 100}
	pumpBucketOps(ops, &rateLimit{})

	eventCount := verifyBucketOps(ops.count, statsLookupRetryCounter)
	if ops.count != eventCount {
		c.testHandle.Error("For", c.testName,
			"expected", ops.count,
			"got", eventCount,
		)
	}
	flushFunctionAndBucket(c.handler)
}

// HEAD
func (c *curlTester) testHead() {
	loBinding := common.Curl{
		Hostname:               "http://localhost:9090/head",
		Value:                  "localhost",
		ValidateSSLCertificate: false,
		AuthType:               "no-auth",
		AllowCookies:           false,
	}

	c.settings = &commonSettings{curlBindings: []common.Curl{loBinding}}
	c.test()
}

// HEAD + auth
func (c *curlTester) testHeadBasicAuth() {
	loBinding := common.Curl{
		Hostname:               "http://localhost:9090/head/auth",
		Value:                  "localhost",
		ValidateSSLCertificate: false,
		AuthType:               "basic",
		AllowCookies:           false,
		Username:               "Administrator",
		Password:               "asdasd",
	}
	c.settings = &commonSettings{curlBindings: []common.Curl{loBinding}}
	c.test()
}

func (c *curlTester) testHeadDigestAuth() {
	loBinding := common.Curl{
		Hostname:               "http://localhost:9090/head/auth/digest",
		Value:                  "localhost",
		ValidateSSLCertificate: false,
		AuthType:               "digest",
		AllowCookies:           false,
		Username:               "Administrator",
		Password:               "asdasd",
	}
	c.settings = &commonSettings{curlBindings: []common.Curl{loBinding}}
	c.test()
}

// GET
func (c *curlTester) testGet() {
	loBinding := common.Curl{
		Hostname:               "http://localhost:9090/get",
		Value:                  "localhost",
		ValidateSSLCertificate: false,
		AuthType:               "no-auth",
		AllowCookies:           false,
	}

	c.settings = &commonSettings{curlBindings: []common.Curl{loBinding}}
	c.test()
}

func (c *curlTester) testEmpty() {
	loBinding := common.Curl{
		Hostname:               "http://localhost:9090/empty",
		Value:                  "localhost",
		ValidateSSLCertificate: false,
		AuthType:               "no-auth",
		AllowCookies:           false,
	}

	c.settings = &commonSettings{curlBindings: []common.Curl{loBinding}}
	c.test()
}

// GET + auth
func (c *curlTester) testGetBasicAuth() {
	loBinding := common.Curl{
		Hostname:               "http://localhost:9090/get/auth",
		Value:                  "localhost",
		ValidateSSLCertificate: false,
		AuthType:               "basic",
		AllowCookies:           false,
		Username:               "Administrator",
		Password:               "asdasd",
	}
	c.settings = &commonSettings{curlBindings: []common.Curl{loBinding}}
	c.test()
}

func (c *curlTester) testGetDigestAuth() {
	loBinding := common.Curl{
		Hostname:               "http://localhost:9090/get/auth/digest",
		Value:                  "localhost",
		ValidateSSLCertificate: false,
		AuthType:               "digest",
		AllowCookies:           false,
		Username:               "Administrator",
		Password:               "asdasd",
	}
	c.settings = &commonSettings{curlBindings: []common.Curl{loBinding}}
	c.test()
}

// GET + cookie
func (c *curlTester) testGetCookie() {
	loBinding := common.Curl{
		Hostname:               "http://localhost:9090/get",
		Value:                  "localhost",
		ValidateSSLCertificate: false,
		AuthType:               "no-auth",
		AllowCookies:           true,
	}
	c.settings = &commonSettings{curlBindings: []common.Curl{loBinding}}
	c.test()
}

// GET + auth + cookie
func (c *curlTester) testGetBasicAuthCookie() {
	loBinding := common.Curl{
		Hostname:               "http://localhost:9090/get/auth",
		Value:                  "localhost",
		ValidateSSLCertificate: false,
		AuthType:               "basic",
		AllowCookies:           true,
		Username:               "Administrator",
		Password:               "asdasd",
	}
	c.settings = &commonSettings{curlBindings: []common.Curl{loBinding}}
	c.test()
}

func (c *curlTester) testGetDigestAuthCookie() {
	loBinding := common.Curl{
		Hostname:               "http://localhost:9090/get/auth/digest",
		Value:                  "localhost",
		ValidateSSLCertificate: false,
		AuthType:               "digest",
		AllowCookies:           true,
		Username:               "Administrator",
		Password:               "asdasd",
	}
	c.settings = &commonSettings{curlBindings: []common.Curl{loBinding}}
	c.test()
}

// POST
func (c *curlTester) testPost() {
	loBinding := common.Curl{
		Hostname:               "http://localhost:9090/post",
		Value:                  "localhost",
		ValidateSSLCertificate: false,
		AuthType:               "no-auth",
		AllowCookies:           false,
	}

	c.settings = &commonSettings{curlBindings: []common.Curl{loBinding}}
	c.test()
}

// POST + auth
func (c *curlTester) testPostDigestAuth() {
	loBinding := common.Curl{
		Hostname:               "http://localhost:9090/post/auth/digest",
		Value:                  "localhost",
		ValidateSSLCertificate: false,
		AuthType:               "digest",
		AllowCookies:           false,
		Username:               "Administrator",
		Password:               "asdasd",
	}
	c.settings = &commonSettings{curlBindings: []common.Curl{loBinding}}
	c.test()
}

func (c *curlTester) testPostBasicAuth() {
	loBinding := common.Curl{
		Hostname:               "http://localhost:9090/post/auth",
		Value:                  "localhost",
		ValidateSSLCertificate: false,
		AuthType:               "basic",
		AllowCookies:           false,
		Username:               "Administrator",
		Password:               "asdasd",
	}
	c.settings = &commonSettings{curlBindings: []common.Curl{loBinding}}
	c.test()
}

// POST + cookie
func (c *curlTester) testPostCookie() {
	loBinding := common.Curl{
		Hostname:               "http://localhost:9090/post",
		Value:                  "localhost",
		ValidateSSLCertificate: false,
		AuthType:               "no-auth",
		AllowCookies:           true,
	}
	c.settings = &commonSettings{curlBindings: []common.Curl{loBinding}}
	c.test()
}

// POST + auth + cookie
func (c *curlTester) testPostBasicAuthCookie() {
	loBinding := common.Curl{
		Hostname:               "http://localhost:9090/post/auth",
		Value:                  "localhost",
		ValidateSSLCertificate: false,
		AuthType:               "basic",
		AllowCookies:           true,
		Username:               "Administrator",
		Password:               "asdasd",
	}
	c.settings = &commonSettings{curlBindings: []common.Curl{loBinding}}
	c.test()
}

func (c *curlTester) testPostDigestAuthCookie() {
	loBinding := common.Curl{
		Hostname:               "http://localhost:9090/post/auth/digest",
		Value:                  "localhost",
		ValidateSSLCertificate: false,
		AuthType:               "digest",
		AllowCookies:           true,
		Username:               "Administrator",
		Password:               "asdasd",
	}
	c.settings = &commonSettings{curlBindings: []common.Curl{loBinding}}
	c.test()
}

// PUT
func (c *curlTester) testPut() {
	loBinding := common.Curl{
		Hostname:               "http://localhost:9090/put",
		Value:                  "localhost",
		ValidateSSLCertificate: false,
		AuthType:               "no-auth",
		AllowCookies:           false,
	}

	c.settings = &commonSettings{curlBindings: []common.Curl{loBinding}}
	c.test()
}

// PUT + auth
func (c *curlTester) testPutBasicAuth() {
	loBinding := common.Curl{
		Hostname:               "http://localhost:9090/put/auth",
		Value:                  "localhost",
		ValidateSSLCertificate: false,
		AuthType:               "basic",
		AllowCookies:           false,
		Username:               "Administrator",
		Password:               "asdasd",
	}
	c.settings = &commonSettings{curlBindings: []common.Curl{loBinding}}
	c.test()
}

func (c *curlTester) testPutDigestAuth() {
	loBinding := common.Curl{
		Hostname:               "http://localhost:9090/put/auth/digest",
		Value:                  "localhost",
		ValidateSSLCertificate: false,
		AuthType:               "digest",
		AllowCookies:           false,
		Username:               "Administrator",
		Password:               "asdasd",
	}
	c.settings = &commonSettings{curlBindings: []common.Curl{loBinding}}
	c.test()
}

// PUT + cookie
func (c *curlTester) testPutCookie() {
	loBinding := common.Curl{
		Hostname:               "http://localhost:9090/put",
		Value:                  "localhost",
		ValidateSSLCertificate: false,
		AuthType:               "no-auth",
		AllowCookies:           true,
	}
	c.settings = &commonSettings{curlBindings: []common.Curl{loBinding}}
	c.test()
}

// PUT + auth + cookie
func (c *curlTester) testPutDigestAuthCookie() {
	loBinding := common.Curl{
		Hostname:               "http://localhost:9090/put/auth/digest",
		Value:                  "localhost",
		ValidateSSLCertificate: false,
		AuthType:               "digest",
		AllowCookies:           true,
		Username:               "Administrator",
		Password:               "asdasd",
	}
	c.settings = &commonSettings{curlBindings: []common.Curl{loBinding}}
	c.test()
}

func (c *curlTester) testPutBasicAuthCookie() {
	loBinding := common.Curl{
		Hostname:               "http://localhost:9090/put/auth",
		Value:                  "localhost",
		ValidateSSLCertificate: false,
		AuthType:               "basic",
		AllowCookies:           true,
		Username:               "Administrator",
		Password:               "asdasd",
	}
	c.settings = &commonSettings{curlBindings: []common.Curl{loBinding}}
	c.test()
}

// DELETE
func (c *curlTester) testDelete() {
	loBinding := common.Curl{
		Hostname:               "http://localhost:9090/delete",
		Value:                  "localhost",
		ValidateSSLCertificate: false,
		AuthType:               "no-auth",
		AllowCookies:           false,
	}

	c.settings = &commonSettings{curlBindings: []common.Curl{loBinding}}
	c.test()
}

// DELETE + auth
func (c *curlTester) testDeleteDigestAuth() {
	loBinding := common.Curl{
		Hostname:               "http://localhost:9090/delete/auth/digest",
		Value:                  "localhost",
		ValidateSSLCertificate: false,
		AuthType:               "digest",
		AllowCookies:           false,
		Username:               "Administrator",
		Password:               "asdasd",
	}
	c.settings = &commonSettings{curlBindings: []common.Curl{loBinding}}
	c.test()
}

func (c *curlTester) testDeleteBasicAuth() {
	loBinding := common.Curl{
		Hostname:               "http://localhost:9090/delete/auth",
		Value:                  "localhost",
		ValidateSSLCertificate: false,
		AuthType:               "basic",
		AllowCookies:           false,
		Username:               "Administrator",
		Password:               "asdasd",
	}
	c.settings = &commonSettings{curlBindings: []common.Curl{loBinding}}
	c.test()
}

// DELETE + cookie
func (c *curlTester) testDeleteCookie() {
	loBinding := common.Curl{
		Hostname:               "http://localhost:9090/delete",
		Value:                  "localhost",
		ValidateSSLCertificate: false,
		AuthType:               "no-auth",
		AllowCookies:           true,
	}
	c.settings = &commonSettings{curlBindings: []common.Curl{loBinding}}
	c.test()
}

// DELETE + auth + cookie
func (c *curlTester) testDeleteBasicAuthCookie() {
	loBinding := common.Curl{
		Hostname:               "http://localhost:9090/delete/auth",
		Value:                  "localhost",
		ValidateSSLCertificate: false,
		AuthType:               "basic",
		AllowCookies:           true,
		Username:               "Administrator",
		Password:               "asdasd",
	}
	c.settings = &commonSettings{curlBindings: []common.Curl{loBinding}}
	c.test()
}

func (c *curlTester) testDeleteDigestAuthCookie() {
	loBinding := common.Curl{
		Hostname:               "http://localhost:9090/delete/auth/digest",
		Value:                  "localhost",
		ValidateSSLCertificate: false,
		AuthType:               "digest",
		AllowCookies:           true,
		Username:               "Administrator",
		Password:               "asdasd",
	}
	c.settings = &commonSettings{curlBindings: []common.Curl{loBinding}}
	c.test()
}

func (c *curlTester) testLargeBody() {
	loBinding := common.Curl{
		Hostname:               "http://localhost:9090/large",
		Value:                  "localhost",
		ValidateSSLCertificate: false,
		AuthType:               "no-auth",
		AllowCookies:           false,
	}

	c.settings = &commonSettings{curlBindings: []common.Curl{loBinding}}
	c.test()
}
