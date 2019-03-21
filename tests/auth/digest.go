package auth

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"strings"
)

// Implemented the algorithm outlined here - https://en.wikipedia.org/wiki/Digest_access_authentication

type DigestAuth struct {
	Method   string
	Realm    string
	Qop      string
	Nonce    string
	Opaque   string
	Password string
}

func (d *DigestAuth) Validate(request *DigestRequest) bool {
	a1 := getMD5(fmt.Sprintf("%s:%s:%s", request.Username, d.Realm, d.Password))
	a2 := getMD5(fmt.Sprintf("%s:%s", d.Method, request.Uri))
	expectedHash := getMD5(fmt.Sprintf("%s:%s:%s:%s:%s:%s", a1, d.Nonce, request.Nc, request.Cnonce, d.Qop, a2))
	return request.Response == expectedHash
}

func (d *DigestAuth) GetHeader() string {
	return fmt.Sprintf(`Digest realm="%s", qop="%s", nonce="%s", opaque="%s"`, d.Realm, d.Qop, d.Nonce, d.Opaque)
}

type DigestRequest struct {
	Username string
	Realm    string
	Nonce    string
	Uri      string
	Cnonce   string
	Nc       string
	Qop      string
	Response string
	Opaque   string
}

func NewDigestRequest(authorization string) *DigestRequest {
	fields := parseAuthorization(authorization)
	return &DigestRequest{
		Nonce:    fields["nonce"],
		Cnonce:   fields["cnonce"],
		Nc:       fields["nc"],
		Qop:      fields["qop"],
		Username: fields["username"],
		Realm:    fields["realm"],
		Uri:      fields["uri"],
		Response: fields["response"],
		Opaque:   fields["opaque"],
	}
}

func getMD5(text string) string {
	hasher := md5.New()
	hasher.Write([]byte(text))
	return hex.EncodeToString(hasher.Sum(nil))
}

func trimEnclosingQuotes(s string) string {
	if len(s) == 0 || len(s) == 1 {
		return s
	}

	if strings.HasPrefix(s, `"`) && strings.HasSuffix(s, `"`) {
		return s[1 : len(s)-1]
	}
	return s
}

func parseAuthorization(authorization string) (fields map[string]string) {
	fields = make(map[string]string)
	fieldsList := strings.Split(authorization, ", ")
	if len(fieldsList) == 0 {
		return
	}

	fieldsList[0] = strings.Split(fieldsList[0], " ")[1]
	for i := 0; i < len(fieldsList); i++ {
		field := fieldsList[i]
		eqIndex := strings.Index(field, "=")
		if eqIndex == -1 {
			continue
		}
		fields[field[:eqIndex]] = trimEnclosingQuotes(field[eqIndex+1:])
	}
	return
}
