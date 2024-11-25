package authenticator

import (
	"crypto/rand"
	"fmt"
	"net/http"
	"strings"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/eventing/authenticator/rbac"
)

var (
	clusterURL = ""

	testUser     = ""
	testPassword = ""
)

// For testing purpose only
func TestSetUserPassword(user, password string) {
	testUser = user
	testPassword = password
}

// InitAuthenticator initilaise the authenticator module
// Should call this method before any other method is invoked
// TODO: Maybe use some other better way to set it
func InitAuthenticator(url string) {
	clusterURL = StripScheme(url)
	rbac.InitRbacManager(clusterURL)
}

func DefaultAuthHandler(req *http.Request) {
	AuthHandler(req, nil)
}

func AuthHandler(req *http.Request, authenticator cbauth.Authenticator) {
	cbauth.SetRequestAuthVia(req, authenticator)
}

func GetClusterAuth() (string, string) {
	if testUser != "" && testPassword != "" {
		return testUser, testPassword
	}

	user, password, err := ServiceHttpAuth(clusterURL)
	if err != nil {
		return "", ""
	}
	return user, password
}

func ServiceHttpAuth(clusterURL string) (user string, passwd string, err error) {
	strippedEndpoint := StripScheme(clusterURL)
	user, passwd, err = cbauth.GetHTTPServiceAuth(strippedEndpoint)
	if err != nil {
		err = fmt.Errorf("failed to get cluster auth details, err: %v", err)
		return
	}
	return
}

func GenerateLocalIDs() (string, string, error) {
	dict := []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	buf := make([]byte, 512)
	_, err := rand.Read(buf)
	if err != nil {
		return "", "", fmt.Errorf("error in creating username and password: %v", err)
	}
	for i := 0; i < len(buf); i++ {
		pos := int(buf[i]) % len(dict)
		buf[i] = dict[pos]
	}
	mid := len(buf) / 2
	return string(buf[:mid]), string(buf[mid:]), nil
}

func GetMemcachedServiceAuth(url string) (user string, passwd string, err error) {
	strippedEndpoint := StripScheme(url)
	user, passwd, err = cbauth.GetMemcachedServiceAuth(strippedEndpoint)
	if err != nil {
		err = fmt.Errorf("failed to get memcached auth, err: %v", err)
		return
	}
	return

}

func StripScheme(endpoint string) string {
	return strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
}
