package auth

import (
	"encoding/base64"
	"fmt"
	"github.com/pkg/errors"
	"strings"
)

type BasicAuth struct {
	Username string
	Password string
}

func NewBasicAuth(authorization string) (*BasicAuth, error) {
	credentialsBase64 := strings.Split(authorization, " ")
	if len(credentialsBase64) != 2 {
		return nil, errors.New("Invalid Authorization header format")
	}

	authType := credentialsBase64[0]
	if authType != "Basic" {
		return nil, errors.New("Expected Basic auth")
	}

	userPwd, err := base64.StdEncoding.DecodeString(credentialsBase64[1])
	if err != nil {
		return nil, fmt.Errorf("Unable to decode credentials, err : %v\n", err)
	}

	userPwdPair := strings.Split(string(userPwd), ":")
	if len(userPwdPair) != 2 {
		return nil, errors.New("Expected user:password")
	}
	return &BasicAuth{
		Username: userPwdPair[0],
		Password: userPwdPair[1],
	}, nil
}

func (b *BasicAuth) Validate(other *BasicAuth) bool {
	return b.Username == other.Username && b.Password == other.Password
}
