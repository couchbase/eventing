// +build all curl

package eventing

import (
	"log"
	"net/http"
	"testing"
)

func TestMain(m *testing.M) {
	server := startHttpServer()
	m.Run()
	if err := server.Shutdown(nil); err != nil {
		log.Printf("Unable to shutdown server, err : %v\n", err)
	}
}

func startHttpServer() *http.Server {
	server := &http.Server{Addr: ":9090"}

	http.HandleFunc("/empty", emptyHandler)
	http.HandleFunc("/large", largeHandler)
	http.HandleFunc("/get", getOrDeleteHandler)
	http.HandleFunc("/get/", getOrDeleteHandler)
	http.HandleFunc("/delete", getOrDeleteHandler)
	http.HandleFunc("/delete/", getOrDeleteHandler)
	http.HandleFunc("/post", postOrPutHandler)
	http.HandleFunc("/post/", postOrPutHandler)
	http.HandleFunc("/put", postOrPutHandler)
	http.HandleFunc("/put/", postOrPutHandler)
	http.HandleFunc("/head", headHandler)
	http.HandleFunc("/head/", headHandler)

	go func() {
		err := server.ListenAndServe()
		if err != nil {
			log.Printf("Unable to start the HTTP Server, err : %v", err)
		}
	}()
	return server
}

// HEAD
func TestCurlHead(t *testing.T) {
	curl := curlTester{
		handler:    "curl_head",
		testName:   "TestCurlHead",
		testHandle: t,
	}
	curl.testHead()
}

// HEAD + auth
func TestCurlHeadDigestAuth(t *testing.T) {
	curl := curlTester{
		handler:    "curl_head",
		testName:   "TestCurlHeadDigestAuth",
		testHandle: t,
	}
	curl.testHeadDigestAuth()
}

func TestCurlHeadBasicAuth(t *testing.T) {
	curl := curlTester{
		handler:    "curl_head",
		testName:   "TestCurlHeadBasicAuth",
		testHandle: t,
	}
	curl.testHeadBasicAuth()
}

// GET + types
func TestCurlGetBinary(t *testing.T) {
	curl := curlTester{
		handler:    "curl_get_binary",
		testName:   "TestCurlGetBinary",
		testHandle: t,
	}
	curl.testGet()
}

func TestCurlGetForm(t *testing.T) {
	curl := curlTester{
		handler:    "curl_get_form",
		testName:   "TestCurlGetForm",
		testHandle: t,
	}
	curl.testGet()
}

func TestCurlGetJSON(t *testing.T) {
	curl := curlTester{
		handler:    "curl_get_json",
		testName:   "TestCurlGetJSON",
		testHandle: t,
	}
	curl.testGet()
}

func TestCurlGetText(t *testing.T) {
	curl := curlTester{
		handler:    "curl_get_text",
		testName:   "TestCurlGetText",
		testHandle: t,
	}
	curl.testGet()
}

// GET + types + auth
func TestCurlGetJSONDigestAuth(t *testing.T) {
	curl := curlTester{
		handler:    "curl_get_json",
		testName:   "TestCurlGetJSONDigestAuth",
		testHandle: t,
	}
	curl.testGetDigestAuth()
}

func TestCurlGetFormDigestAuth(t *testing.T) {
	curl := curlTester{
		handler:    "curl_get_form",
		testName:   "TestCurlGetFormDigestAuth",
		testHandle: t,
	}
	curl.testGetDigestAuth()
}

func TestCurlGetBinaryDigestAuth(t *testing.T) {
	curl := curlTester{
		handler:    "curl_get_binary",
		testName:   "TestCurlGetBinaryDigestAuth",
		testHandle: t,
	}
	curl.testGetDigestAuth()
}

func TestCurlGetTextDigestAuth(t *testing.T) {
	curl := curlTester{
		handler:    "curl_get_text",
		testName:   "TestCurlGetTextDigestAuth",
		testHandle: t,
	}
	curl.testGetDigestAuth()
}

func TestCurlGetJSONBasicAuth(t *testing.T) {
	curl := curlTester{
		handler:    "curl_get_json",
		testName:   "TestCurlGetJSONBasicAuth",
		testHandle: t,
	}
	curl.testGetBasicAuth()
}

func TestCurlGetFormBasicAuth(t *testing.T) {
	curl := curlTester{
		handler:    "curl_get_form",
		testName:   "TestCurlGetFormBasicAuth",
		testHandle: t,
	}
	curl.testGetBasicAuth()
}

func TestCurlGetBinaryBasicAuth(t *testing.T) {
	curl := curlTester{
		handler:    "curl_get_binary",
		testName:   "TestCurlGetBinaryBasicAuth",
		testHandle: t,
	}
	curl.testGetBasicAuth()
}

func TestCurlGetTextBasicAuth(t *testing.T) {
	curl := curlTester{
		handler:    "curl_get_text",
		testName:   "TestCurlGetTextBasicAuth",
		testHandle: t,
	}
	curl.testGetBasicAuth()
}

// GET + types + cookie
func TestCurlGetJSONCookie(t *testing.T) {
	curl := curlTester{
		handler:    "curl_get_json_cookie",
		testName:   "TestCurlGetJSONCookie",
		testHandle: t,
	}
	curl.testGetCookie()
}

func TestCurlGetBinaryCookie(t *testing.T) {
	curl := curlTester{
		handler:    "curl_get_binary_cookie",
		testName:   "TestCurlGetBinaryCookie",
		testHandle: t,
	}
	curl.testGetCookie()
}

func TestCurlGetTextCookie(t *testing.T) {
	curl := curlTester{
		handler:    "curl_get_text_cookie",
		testName:   "TestCurlGetTextCookie",
		testHandle: t,
	}
	curl.testGetCookie()
}

func TestCurlGetFormCookie(t *testing.T) {
	curl := curlTester{
		handler:    "curl_get_form_cookie",
		testName:   "TestCurlGetFormCookie",
		testHandle: t,
	}
	curl.testGetCookie()
}

// GET + types + auth + cookie
func TestCurlGetJSONDigestAuthCookie(t *testing.T) {
	curl := curlTester{
		handler:    "curl_get_json_cookie",
		testName:   "TestCurlGetJSONDigestAuthCookie",
		testHandle: t,
	}
	curl.testGetDigestAuthCookie()
}

func TestCurlGetTextDigestAuthCookie(t *testing.T) {
	curl := curlTester{
		handler:    "curl_get_json_cookie",
		testName:   "TestCurlGetTextDigestAuthCookie",
		testHandle: t,
	}
	curl.testGetDigestAuthCookie()
}

func TestCurlGetBinaryDigestAuthCookie(t *testing.T) {
	curl := curlTester{
		handler:    "curl_get_binary_cookie",
		testName:   "TestCurlGetBinaryDigestAuthCookie",
		testHandle: t,
	}
	curl.testGetDigestAuthCookie()
}

func TestCurlGetFormDigestAuthCookie(t *testing.T) {
	curl := curlTester{
		handler:    "curl_get_form_cookie",
		testName:   "TestCurlGetFormDigestAuthCookie",
		testHandle: t,
	}
	curl.testGetDigestAuthCookie()
}

func TestCurlGetJSONBasicAuthCookie(t *testing.T) {
	curl := curlTester{
		handler:    "curl_get_json_cookie",
		testName:   "TestCurlGetJSONBasicAuthCookie",
		testHandle: t,
	}
	curl.testGetBasicAuthCookie()
}

func TestCurlGetTextBasicAuthCookie(t *testing.T) {
	curl := curlTester{
		handler:    "curl_get_json_cookie",
		testName:   "TestCurlGetTextBasicAuthCookie",
		testHandle: t,
	}
	curl.testGetBasicAuthCookie()
}

func TestCurlGetBinaryBasicAuthCookie(t *testing.T) {
	curl := curlTester{
		handler:    "curl_get_binary_cookie",
		testName:   "TestCurlGetBinaryBasicAuthCookie",
		testHandle: t,
	}
	curl.testGetBasicAuthCookie()
}

func TestCurlGetFormBasicAuthCookie(t *testing.T) {
	curl := curlTester{
		handler:    "curl_get_form_cookie",
		testName:   "TestCurlGetFormBasicAuthCookie",
		testHandle: t,
	}
	curl.testGetBasicAuthCookie()
}

// POST + types
func TestCurlPostBinary(t *testing.T) {
	curl := curlTester{
		handler:    "curl_post_binary",
		testName:   "TestCurlPostBinary",
		testHandle: t,
	}
	curl.testPost()
}

func TestCurlPostForm(t *testing.T) {
	curl := curlTester{
		handler:    "curl_post_form",
		testName:   "TestCurlPostForm",
		testHandle: t,
	}
	curl.testPost()
}

func TestCurlPostJSON(t *testing.T) {
	curl := curlTester{
		handler:    "curl_post_json",
		testName:   "TestCurlPostJSON",
		testHandle: t,
	}
	curl.testPost()
}

func TestCurlPostText(t *testing.T) {
	curl := curlTester{
		handler:    "curl_post_text",
		testName:   "TestCurlPostText",
		testHandle: t,
	}
	curl.testPost()
}

// POST + types + auth
func TestCurlPostBinaryDigestAuth(t *testing.T) {
	curl := curlTester{
		handler:    "curl_post_binary",
		testName:   "TestCurlPostBinaryDigestAuth",
		testHandle: t,
	}
	curl.testPostDigestAuth()
}

func TestCurlPostFormDigestAuth(t *testing.T) {
	curl := curlTester{
		handler:    "curl_post_form",
		testName:   "TestCurlPostFormDigestAuth",
		testHandle: t,
	}
	curl.testPostDigestAuth()
}

func TestCurlPostJSONDigestAuth(t *testing.T) {
	curl := curlTester{
		handler:    "curl_post_json",
		testName:   "TestCurlPostJSONDigestAuth",
		testHandle: t,
	}
	curl.testPostDigestAuth()
}

func TestCurlPostTextDigestAuth(t *testing.T) {
	curl := curlTester{
		handler:    "curl_post_text",
		testName:   "TestCurlPostTextDigestAuth",
		testHandle: t,
	}
	curl.testPostDigestAuth()
}

func TestCurlPostBinaryBasicAuth(t *testing.T) {
	curl := curlTester{
		handler:    "curl_post_binary",
		testName:   "TestCurlPostBinaryBasicAuth",
		testHandle: t,
	}
	curl.testPostBasicAuth()
}

func TestCurlPostFormBasicAuth(t *testing.T) {
	curl := curlTester{
		handler:    "curl_post_form",
		testName:   "TestCurlPostFormBasicAuth",
		testHandle: t,
	}
	curl.testPostBasicAuth()
}

func TestCurlPostJSONBasicAuth(t *testing.T) {
	curl := curlTester{
		handler:    "curl_post_json",
		testName:   "TestCurlPostJSONBasicAuth",
		testHandle: t,
	}
	curl.testPostBasicAuth()
}

func TestCurlPostTextBasicAuth(t *testing.T) {
	curl := curlTester{
		handler:    "curl_post_text",
		testName:   "TestCurlPostTextBasicAuth",
		testHandle: t,
	}
	curl.testPostBasicAuth()
}

// POST + types + cookie
func TestCurlPostBinaryCookie(t *testing.T) {
	curl := curlTester{
		handler:    "curl_post_binary_cookie",
		testName:   "TestCurlPostBinaryCookie",
		testHandle: t,
	}
	curl.testPostCookie()
}

func TestCurlPostFormCookie(t *testing.T) {
	curl := curlTester{
		handler:    "curl_post_form_cookie",
		testName:   "TestCurlPostFormCookie",
		testHandle: t,
	}
	curl.testPostCookie()
}

func TestCurlPostJSONCookie(t *testing.T) {
	curl := curlTester{
		handler:    "curl_post_json_cookie",
		testName:   "TestCurlPostJSONCookie",
		testHandle: t,
	}
	curl.testPostCookie()
}

func TestCurlPostTextCookie(t *testing.T) {
	curl := curlTester{
		handler:    "curl_post_text_cookie",
		testName:   "TestCurlPostTextCookie",
		testHandle: t,
	}
	curl.testPostCookie()
}

// POST + types + auth + cookie
func TestCurlPostBinaryDigestAuthCookie(t *testing.T) {
	curl := curlTester{
		handler:    "curl_post_binary_cookie",
		testName:   "TestCurlPostBinaryDigestAuthCookie",
		testHandle: t,
	}
	curl.testPostDigestAuthCookie()
}

func TestCurlPostFormDigestAuthCookie(t *testing.T) {
	curl := curlTester{
		handler:    "curl_post_form_cookie",
		testName:   "TestCurlPostFormDigestAuthCookie",
		testHandle: t,
	}
	curl.testPostDigestAuthCookie()
}

func TestCurlPostJSONDigestAuthCookie(t *testing.T) {
	curl := curlTester{
		handler:    "curl_post_json_cookie",
		testName:   "TestCurlPostJSONDigestAuthCookie",
		testHandle: t,
	}
	curl.testPostDigestAuthCookie()
}

func TestCurlPostTextDigestAuthCookie(t *testing.T) {
	curl := curlTester{
		handler:    "curl_post_text_cookie",
		testName:   "TestCurlPostTextDigestAuthCookie",
		testHandle: t,
	}
	curl.testPostDigestAuthCookie()
}

func TestCurlPostBinaryBasicAuthCookie(t *testing.T) {
	curl := curlTester{
		handler:    "curl_post_binary_cookie",
		testName:   "TestCurlPostBinaryBasicAuthCookie",
		testHandle: t,
	}
	curl.testPostBasicAuthCookie()
}

func TestCurlPostFormBasicAuthCookie(t *testing.T) {
	curl := curlTester{
		handler:    "curl_post_form_cookie",
		testName:   "TestCurlPostFormBasicAuthCookie",
		testHandle: t,
	}
	curl.testPostBasicAuthCookie()
}

func TestCurlPostJSONBasicAuthCookie(t *testing.T) {
	curl := curlTester{
		handler:    "curl_post_json_cookie",
		testName:   "TestCurlPostJSONBasicAuthCookie",
		testHandle: t,
	}
	curl.testPostBasicAuthCookie()
}

func TestCurlPostTextBasicAuthCookie(t *testing.T) {
	curl := curlTester{
		handler:    "curl_post_text_cookie",
		testName:   "TestCurlPostTextBasicAuthCookie",
		testHandle: t,
	}
	curl.testPostBasicAuthCookie()
}

// PUT + types
func TestCurlPutBinary(t *testing.T) {
	curl := curlTester{
		handler:    "curl_put_binary",
		testName:   "TestCurlPutBinary",
		testHandle: t,
	}
	curl.testPut()
}

func TestCurlPutForm(t *testing.T) {
	curl := curlTester{
		handler:    "curl_put_form",
		testName:   "TestCurlPutForm",
		testHandle: t,
	}
	curl.testPut()
}

func TestCurlPutJSON(t *testing.T) {
	curl := curlTester{
		handler:    "curl_put_json",
		testName:   "TestCurlPutJSON",
		testHandle: t,
	}
	curl.testPut()
}

func TestCurlPutText(t *testing.T) {
	curl := curlTester{
		handler:    "curl_put_text",
		testName:   "TestCurlPutText",
		testHandle: t,
	}
	curl.testPut()
}

// PUT + auth
func TestCurlPutBinaryDigestAuth(t *testing.T) {
	curl := curlTester{
		handler:    "curl_put_binary",
		testName:   "TestCurlPutBinaryDigestAuth",
		testHandle: t,
	}
	curl.testPutDigestAuth()
}

func TestCurlPutFormDigestAuth(t *testing.T) {
	curl := curlTester{
		handler:    "curl_put_form",
		testName:   "TestCurlPutFormDigestAuth",
		testHandle: t,
	}
	curl.testPutDigestAuth()
}

func TestCurlPutJSONDigestAuth(t *testing.T) {
	curl := curlTester{
		handler:    "curl_put_json",
		testName:   "TestCurlPutJSONDigestAuth",
		testHandle: t,
	}
	curl.testPutDigestAuth()
}

func TestCurlPutTextDigestAuth(t *testing.T) {
	curl := curlTester{
		handler:    "curl_put_text",
		testName:   "TestCurlPutTextDigestAuth",
		testHandle: t,
	}
	curl.testPutDigestAuth()
}

func TestCurlPutBinaryBasicAuth(t *testing.T) {
	curl := curlTester{
		handler:    "curl_put_binary",
		testName:   "TestCurlPutBinaryBasicAuth",
		testHandle: t,
	}
	curl.testPutBasicAuth()
}

func TestCurlPutFormBasicAuth(t *testing.T) {
	curl := curlTester{
		handler:    "curl_put_form",
		testName:   "TestCurlPutFormBasicAuth",
		testHandle: t,
	}
	curl.testPutBasicAuth()
}

func TestCurlPutJSONBasicAuth(t *testing.T) {
	curl := curlTester{
		handler:    "curl_put_json",
		testName:   "TestCurlPutJSONBasicAuth",
		testHandle: t,
	}
	curl.testPutBasicAuth()
}

func TestCurlPutTextBasicAuth(t *testing.T) {
	curl := curlTester{
		handler:    "curl_put_text",
		testName:   "TestCurlPutTextBasicAuth",
		testHandle: t,
	}
	curl.testPutBasicAuth()
}

// PUT + types + cookie
func TestCurlPutBinaryCookie(t *testing.T) {
	curl := curlTester{
		handler:    "curl_put_binary_cookie",
		testName:   "TestCurlPutBinaryCookie",
		testHandle: t,
	}
	curl.testPutCookie()
}

func TestCurlPutFormCookie(t *testing.T) {
	curl := curlTester{
		handler:    "curl_put_form_cookie",
		testName:   "TestCurlPutFormCookie",
		testHandle: t,
	}
	curl.testPutCookie()
}

func TestCurlPutJSONCookie(t *testing.T) {
	curl := curlTester{
		handler:    "curl_put_json_cookie",
		testName:   "TestCurlPutJSONCookie",
		testHandle: t,
	}
	curl.testPutCookie()
}

func TestCurlPutTextCookie(t *testing.T) {
	curl := curlTester{
		handler:    "curl_put_text_cookie",
		testName:   "TestCurlPutTextCookie",
		testHandle: t,
	}
	curl.testPutCookie()
}

// PUT + types + auth + cookie
func TestCurlPutBinaryDigestAuthCookie(t *testing.T) {
	curl := curlTester{
		handler:    "curl_put_binary_cookie",
		testName:   "TestCurlPutBinaryDigestAuthCookie",
		testHandle: t,
	}
	curl.testPutDigestAuthCookie()
}

func TestCurlPutFormDigestAuthCookie(t *testing.T) {
	curl := curlTester{
		handler:    "curl_put_form_cookie",
		testName:   "TestCurlPutFormDigestAuthCookie",
		testHandle: t,
	}
	curl.testPutDigestAuthCookie()
}

func TestCurlPutJSONDigestAuthCookie(t *testing.T) {
	curl := curlTester{
		handler:    "curl_put_json_cookie",
		testName:   "TestCurlPutJSONDigestAuthCookie",
		testHandle: t,
	}
	curl.testPutDigestAuthCookie()
}

func TestCurlPutTextDigestAuthCookie(t *testing.T) {
	curl := curlTester{
		handler:    "curl_put_text_cookie",
		testName:   "TestCurlPutTextDigestAuthCookie",
		testHandle: t,
	}
	curl.testPutDigestAuthCookie()
}

func TestCurlPutBinaryBasicAuthCookie(t *testing.T) {
	curl := curlTester{
		handler:    "curl_put_binary_cookie",
		testName:   "TestCurlPutBinaryBasicAuthCookie",
		testHandle: t,
	}
	curl.testPutBasicAuthCookie()
}

func TestCurlPutFormBasicAuthCookie(t *testing.T) {
	curl := curlTester{
		handler:    "curl_put_form_cookie",
		testName:   "TestCurlPutFormBasicAuthCookie",
		testHandle: t,
	}
	curl.testPutBasicAuthCookie()
}

func TestCurlPutJSONBasicAuthCookie(t *testing.T) {
	curl := curlTester{
		handler:    "curl_put_json_cookie",
		testName:   "TestCurlPutJSONBasicAuthCookie",
		testHandle: t,
	}
	curl.testPutBasicAuthCookie()
}

func TestCurlPutTextBasicAuthCookie(t *testing.T) {
	curl := curlTester{
		handler:    "curl_put_text_cookie",
		testName:   "TestCurlPutTextBasicAuthCookie",
		testHandle: t,
	}
	curl.testPutBasicAuthCookie()
}

// DELETE + types
func TestCurlDeleteBinary(t *testing.T) {
	curl := curlTester{
		handler:    "curl_delete_binary",
		testName:   "TestCurlDeleteBinary",
		testHandle: t,
	}
	curl.testDelete()
}

func TestCurlDeleteForm(t *testing.T) {
	curl := curlTester{
		handler:    "curl_delete_form",
		testName:   "TestCurlDeleteForm",
		testHandle: t,
	}
	curl.testDelete()
}

func TestCurlDeleteJSON(t *testing.T) {
	curl := curlTester{
		handler:    "curl_delete_json",
		testName:   "TestCurlDeleteJSON",
		testHandle: t,
	}
	curl.testDelete()
}

func TestCurlDeleteText(t *testing.T) {
	curl := curlTester{
		handler:    "curl_delete_text",
		testName:   "TestCurlDeleteText",
		testHandle: t,
	}
	curl.testDelete()
}

// DELETE + types + cookie
func TestCurlDeleteBinaryCookie(t *testing.T) {
	curl := curlTester{
		handler:    "curl_delete_binary_cookie",
		testName:   "TestCurlDeleteBinaryCookie",
		testHandle: t,
	}
	curl.testDeleteCookie()
}

func TestCurlDeleteFormCookie(t *testing.T) {
	curl := curlTester{
		handler:    "curl_delete_form_cookie",
		testName:   "TestCurlDeleteFormCookie",
		testHandle: t,
	}
	curl.testDeleteCookie()
}

func TestCurlDeleteJSONCookie(t *testing.T) {
	curl := curlTester{
		handler:    "curl_delete_json_cookie",
		testName:   "TestCurlDeleteJSONCookie",
		testHandle: t,
	}
	curl.testDeleteCookie()
}

func TestCurlDeleteTextCookie(t *testing.T) {
	curl := curlTester{
		handler:    "curl_delete_text_cookie",
		testName:   "TestCurlDeleteTextCookie",
		testHandle: t,
	}
	curl.testDeleteCookie()
}

// DELETE + types + auth
func TestCurlDeleteBinaryDigestAuth(t *testing.T) {
	curl := curlTester{
		handler:    "curl_delete_binary",
		testName:   "TestCurlDeleteBinaryDigestAuth",
		testHandle: t,
	}
	curl.testDeleteDigestAuth()
}

func TestCurlDeleteFormDigestAuth(t *testing.T) {
	curl := curlTester{
		handler:    "curl_delete_form",
		testName:   "TestCurlDeleteFormDigestAuth",
		testHandle: t,
	}
	curl.testDeleteDigestAuth()
}

func TestCurlDeleteJSONDigestAuth(t *testing.T) {
	curl := curlTester{
		handler:    "curl_delete_json",
		testName:   "TestCurlDeleteJSONDigestAuth",
		testHandle: t,
	}
	curl.testDeleteDigestAuth()
}

func TestCurlDeleteTextDigestAuth(t *testing.T) {
	curl := curlTester{
		handler:    "curl_delete_text",
		testName:   "TestCurlDeleteTextDigestAuth",
		testHandle: t,
	}
	curl.testDeleteDigestAuth()
}

func TestCurlDeleteBinaryBasicAuth(t *testing.T) {
	curl := curlTester{
		handler:    "curl_delete_binary",
		testName:   "TestCurlDeleteBinaryBasicAuth",
		testHandle: t,
	}
	curl.testDeleteBasicAuth()
}

func TestCurlDeleteFormBasicAuth(t *testing.T) {
	curl := curlTester{
		handler:    "curl_delete_form",
		testName:   "TestCurlDeleteFormBasicAuth",
		testHandle: t,
	}
	curl.testDeleteBasicAuth()
}

func TestCurlDeleteJSONBasicAuth(t *testing.T) {
	curl := curlTester{
		handler:    "curl_delete_json",
		testName:   "TestCurlDeleteJSONBasicAuth",
		testHandle: t,
	}
	curl.testDeleteBasicAuth()
}

func TestCurlDeleteTextBasicAuth(t *testing.T) {
	curl := curlTester{
		handler:    "curl_delete_text",
		testName:   "TestCurlDeleteTextBasicAuth",
		testHandle: t,
	}
	curl.testDeleteBasicAuth()
}

// DELETE + types + auth + cookie
func TestCurlDeleteBinaryDigestAuthCookie(t *testing.T) {
	curl := curlTester{
		handler:    "curl_delete_binary_cookie",
		testName:   "TestCurlDeleteBinaryDigestAuthCookie",
		testHandle: t,
	}
	curl.testDeleteDigestAuthCookie()
}

func TestCurlDeleteFormDigestAuthCookie(t *testing.T) {
	curl := curlTester{
		handler:    "curl_delete_form_cookie",
		testName:   "TestCurlDeleteFormDigestAuthCookie",
		testHandle: t,
	}
	curl.testDeleteDigestAuthCookie()
}

func TestCurlDeleteJSONDigestAuthCookie(t *testing.T) {
	curl := curlTester{
		handler:    "curl_delete_json_cookie",
		testName:   "TestCurlDeleteJSONDigestAuthCookie",
		testHandle: t,
	}
	curl.testDeleteDigestAuthCookie()
}

func TestCurlDeleteTextDigestAuthCookie(t *testing.T) {
	curl := curlTester{
		handler:    "curl_delete_text_cookie",
		testName:   "TestCurlDeleteTextDigestAuthCookie",
		testHandle: t,
	}
	curl.testDeleteDigestAuthCookie()
}

func TestCurlDeleteBinaryBasicAuthCookie(t *testing.T) {
	curl := curlTester{
		handler:    "curl_delete_binary_cookie",
		testName:   "TestCurlDeleteBinaryBasicAuthCookie",
		testHandle: t,
	}
	curl.testDeleteBasicAuthCookie()
}

func TestCurlDeleteFormBasicAuthCookie(t *testing.T) {
	curl := curlTester{
		handler:    "curl_delete_form_cookie",
		testName:   "TestCurlDeleteFormBasicAuthCookie",
		testHandle: t,
	}
	curl.testDeleteBasicAuthCookie()
}

func TestCurlDeleteJSONBasicAuthCookie(t *testing.T) {
	curl := curlTester{
		handler:    "curl_delete_json_cookie",
		testName:   "TestCurlDeleteJSONBasicAuthCookie",
		testHandle: t,
	}
	curl.testDeleteBasicAuthCookie()
}

func TestCurlDeleteTextBasicAuthCookie(t *testing.T) {
	curl := curlTester{
		handler:    "curl_delete_text_cookie",
		testName:   "TestCurlDeleteTextBasicAuthCookie",
		testHandle: t,
	}
	curl.testDeleteBasicAuthCookie()
}

// Method + empty body
func TestCurlGetEmptyJSON(t *testing.T) {
	curl := curlTester{
		handler:    "curl_get_empty_json",
		testName:   "TestCurlGetEmptyJSON",
		testHandle: t,
	}
	curl.testEmpty()
}

func TestCurlGetEmptyBinary(t *testing.T) {
	curl := curlTester{
		handler:    "curl_get_empty_binary",
		testName:   "TestCurlGetEmptyBinary",
		testHandle: t,
	}
	curl.testEmpty()
}

func TestCurlGetEmptyText(t *testing.T) {
	curl := curlTester{
		handler:    "curl_get_empty_text",
		testName:   "TestCurlGetEmptyText",
		testHandle: t,
	}
	curl.testEmpty()
}

func TestCurlGetEmptyForm(t *testing.T) {
	curl := curlTester{
		handler:    "curl_get_empty_form",
		testName:   "TestCurlGetEmptyForm",
		testHandle: t,
	}
	curl.testEmpty()
}

func TestCurlPostEmpty(t *testing.T) {
	curl := curlTester{
		handler:    "curl_post_empty",
		testName:   "TestCurlPostEmpty",
		testHandle: t,
	}
	curl.testEmpty()
}

func TestCurlPutEmpty(t *testing.T) {
	curl := curlTester{
		handler:    "curl_put_empty",
		testName:   "TestCurlPutEmpty",
		testHandle: t,
	}
	curl.testEmpty()
}

func TestCurlDeleteEmpty(t *testing.T) {
	curl := curlTester{
		handler:    "curl_delete_empty",
		testName:   "TestCurlDeleteEmpty",
		testHandle: t,
	}
	curl.testEmpty()
}

// Miscellaneous
func TestCurlGetMalformedJSON(t *testing.T) {
	curl := curlTester{
		handler:    "curl_get_malformed_json",
		testName:   "TestCurlGetMalformedJSON",
		testHandle: t,
	}
	curl.testGet()
}

func TestCurlGetParams(t *testing.T) {
	curl := curlTester{
		handler:    "curl_get_params",
		testName:   "TestCurlGetParams",
		testHandle: t,
	}
	curl.testGet()
}

func TestCurlGetUnknownType(t *testing.T) {
	curl := curlTester{
		handler:    "curl_get_unknown_type",
		testName:   "TestCurlGetUnknownType",
		testHandle: t,
	}
	curl.testGet()
}

func TestCurlPostLargeJSON(t *testing.T) {
	curl := curlTester{
		handler:    "curl_post_large_json",
		testName:   "TestCurlPostLargeJSON",
		testHandle: t,
	}
	curl.testLargeBody()
}

func TestCurlPutLargeJSON(t *testing.T) {
	curl := curlTester{
		handler:    "curl_put_large_json",
		testName:   "TestCurlPutLargeJSON",
		testHandle: t,
	}
	curl.testLargeBody()
}

func TestCurlGetLargeJSON(t *testing.T) {
	curl := curlTester{
		handler:    "curl_get_large_json",
		testName:   "TestCurlGetLargeJSON",
		testHandle: t,
	}
	curl.testLargeBody()
}
