package eventing

import (
	"net/http"
	"net/url"
)

type Verification struct {
	IsBodyConsistent   bool `json:"is_body_consistent"`
	IsCookieConsistent bool `json:"is_cookie_consistent"`
}

func (v *Verification) verifyCookie(r *http.Request) bool {
	cookieKey := "cookie-key"
	expected := "cookie-value"

	actual, err := r.Cookie(cookieKey)
	if err != nil {
		return false
	}
	return actual.Value == expected
}

func (v *Verification) HandleCookie(r *http.Request) {
	if v.verifyCookie(r) {
		v.IsCookieConsistent = true
	}
}

func (v *Verification) verifyBinary(body []byte) bool {
	expected := []uint8{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 42, 0, 0, 0}
	actual := []uint8(body)

	if len(actual) != len(expected) {
		return false
	}

	for i := 0; i < len(actual); i++ {
		if actual[i] != expected[i] {
			return false
		}
	}
	return true
}

func (v *Verification) HandleBinary(body []byte) {
	if v.verifyBinary(body) {
		v.IsBodyConsistent = true
	}
}

func (v *Verification) verifyForm(body []byte) bool {
	actual, err := url.QueryUnescape(string(body))
	if err != nil {
		return false
	}
	expected := `1={"nested-key":"nested-value"}&2=0=this&1=is&2=an&3=array&key=value&another-key=another-value`
	return actual == expected
}

func (v *Verification) HandleForm(body []byte) {
	if v.verifyForm(body) {
		v.IsBodyConsistent = true
	}
}

func (v *Verification) verifyJSON(body []byte) bool {
	expected := `{"key":"here comes some value from Couchbase Function"}`
	actual := string(body)
	return actual == expected
}

func (v *Verification) HandleJSON(body []byte) {
	if v.verifyJSON(body) {
		v.IsBodyConsistent = true
	}
}

func (v *Verification) verifyText(body []byte) bool {
	expected := "here comes some text from Couchbase Function"
	actual := string(body)
	return actual == expected
}

func (v *Verification) HandleText(body []byte) {
	if v.verifyText(body) {
		v.IsBodyConsistent = true
	}
}
