# Introduction

Curl provides a way of interacting with external entities from event handlers. As the handlers can run on multiple nodes, each multi process and multi thread, it can generate very high load on the REST endpoint being targeted. The incoming load to the handler and the external HTTP endpoint being accessed must be carefully sized to ensure the HTTP endpoint can handle the load.

# Features
The following must be supported:

1. Binding for URL
1. Persistent connection for each URL binding
1. HTTP verbs GET, POST, HEAD, PUT, DELETE
1. SSL support
1. Basic and Digest Auth
1. Session cookie support
1. Setting headers (like user-agent, content-type)
1. Username and password as binding
1. Use SSL ciphers provided by cbauth

# Request marshalling
Depending on the type of the variable passed as body, it will be marshalled and passed on to the cURL call, while automatically deducing and filling the `Content-Type` header. Filling the `Content-Type` header will happen only if it has not already been specified.

| Type | Content-Type deduced |
|:------:|:----------------------:|
| JS string | text/plain |
| JS Object | application/json |
| ArrayBuffer | application/octet-stream |

## The `encoding` parameter
The request object has an optional `encoding` parameter which can have one of the following values `FORM`/`JSON`/`TEXT`/`BINARY`.
This serves as a hint to parse the request body. For example, specifying encoding : “FORM” will cause the body to be url-encoded.
> It is to be noted that encoding is an optional parameter and does not have to be specified if it is not necessary. But by specifying this parameter, it implies that the body type must adhere to the encoding, if parsing the body as per encoding fails, an exception will be thrown. For example, if the encoding : "JSON" is specified but the body is not a valid JSON, an exception will be thrown.

# Response unmarshalling
If the `Content-Type` header is present in the response header, we will perform the corresponding unmarshalling action and pass it onto JavaScript handler that invoked the cURL call.

| Content-Type | Unmarshalling action |
|:------------:|:--------------------:|
| text/plain | JS string |
| application/json | JSON.parse() |
| application/x-www-form-urlencoded | url-decode |
| application/octet-stream | ArrayBuffer |

If `Content-Type` is absent in the response header or if the `Content-Type` is not one of those in the above table, we will expose the response body as an `ArrayBuffer` instance and is up to the user to unmarshall the body.

# Session cookies
A session begins when the handler is invoked for execution and is destroyed after the handler execution is done. The cookies of a session are local to each binding.

# API
curl(method, binding[, requestObj])

#### Arguments
1. method - The HTTP method of the cURL request. Must be a string having one of the following values `GET`/`POST`/`PUT`/`HEAD`/`DELETE`.
2. binding - The cURL binding.
3. requestObj - A JavaScript Object having the following structure -
    1. headers : JavaScript Object of Key-Value pairs.
    2. body : The request body to be sent.
    3. encoding : A hint to parse the body. Must be a string having one of the following values - `FORM`/`JSON`/`TEXT`/`BINARY`.
    4. path : This must be a string, it will be appended to the hostname URL of the binding while making the cURL request.
    5. params : This must be a JavaScript Object of Key-Value pairs. This will be url-encoded as Key-Value pairs and appended to the request URL.

#### Returns
A JavaScript Object containing -
1. The response body in a field called `body`.
2. The status code in a field called `status`.
3. The response headers in a field called `headers`.

#### Throws
A JavaScript exception of type `CurlError`, inheriting from the JavaScript `Error` class will be thrown if the curl call could not succeed. This can be handled in a try...catch… block. It will abide by the standard JavaScript error reporting scheme as outlined in https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Error#Custom_Error_Types

# Example
Suppose we want to `POST` to endpoint http://www.example.com/person?name=abc&value=10 with data `{ ‘someKey’ : ‘someValue’ }` and use Basic authentication with username `Administrator` and password as `Couchbase`.

A URL binding needs to be created as follows -

| Hostname | Value |
|:----------:|:-------:|
| http://www.example.com | exampleServer |

| Auth type | Username | Password |
|:---------:|:--------:|:--------:|
| Basic | Administrator | Couchbase |

```javascript
var request = {
	path : '/person',
	params : {
		'name' : 'abc',
		'value' : 10
	},
      body: {
            'someKey': 'someValue'
      }
};

try {
	var response = curl('POST', exampleServer, request);
	log('response body received from server:', response.body);
	log('response headers received from server:', response.headers);
	log('response status received from server:', response.status);
} catch (e) {
	log('error:', e);
}
```
