# JS Evaluator REST API
This API is currently in developer preview and may change before GA. Only functions marked
as BETA are available for us in Couchbase 6.5.0 Beta.

Note that for all calls:
  1. If "name" attribute appears in the body, it must match the name specified in the URL
  2. of "name" is not specified in the body, it defaults to the name specified on the URL
  3. The HTTP Content-Type must be `application/json` for all below REST API calls

## Creating/Updating a function (available in beta)
> POST /functions/v1/libraries/`<library_name>`/functions/`<function_name>`

Note that if the function already exists in the library, it is overwritten. Otherwise the
function is added to the library. If a library does not exist, one will be created.

Only JS functions whose name matches the name of the Couchbase function object are exported.

Below example creates a function by name "adder" in library "math". The function "adder" is
publicly exported, while the function "helper" is not.

> POST /functions/v1/libraries/math/functions/adder
```json
{
  "name": "adder",
  "code": "function adder(a, b) { return a + b; }\n function helper() { return; }"
}
```

## Creating/Updating a library (available in beta)
> POST /functions/v1/libraries/`<function_name>`

Note that if the library exists, the function specified will be added to the existing library.
If a specified function already exists in the old library, it will be overwritten.

Below example creates a library by name "math".

> POST /functions/v1/libraries/math
```json
[
  {
    "name": "math",
    "functions":
    [
      {
        "name": "adder",
        "code": "function adder(a, b) { return a + b; }\n function helper() { return; }"
      }
    ]
  }
]
```

## Creating/Updating a collection of libraries
> POST /functions/v1/libraries

Note that if any specified library exists, the functions specified in the body for that library
will be appened to the existing library. If the function in the library exists in the library
on the server, the function will be overwritten.

Below example adds/updates two libraries, "math" and "science".

> POST /functions/v1/libraries
```json
[
  {
    "name": "math",
    "functions":
    [
      {
        "name": "adder",
        "code": "function adder(a, b) { return a + b; }\n function helper() { return; }"
      }
    ]
  },
  {
    "name": "science",
    "functions":
    [
      {
        "name": "f2c",
        "code": "function f2c(f) { return (5/9)*(f-32); }"
      }
    ]
  }
]
```

## Reading a function (available in beta)
> GET /functions/v1/libraries/`<library_name>`/functions/`<function_name>`

Returns specified function the specified library.

Below example gets function "adder" from library "math".

> GET /functions/v1/libraries/math/functions/adder
```json
{
  "name": "adder",
  "code": "function adder(a, b) { return a + b; }\n function helper() { return; }"
}
```

## Reading a library
> GET /functions/v1/libraries/`<library_name>`

Returns a library with all it's functions.

Below example gets all functions in library "math"

> GET /functions/v1/libraries/math
```json
{
  "name": "math",
  "functions":
  [
    {
      "name": "adder",
      "code": "function adder(a, b) { return a + b; }\n function helper() { return; }"
    }
  ]
}
```

## Reading all libraries
> GET /functions/v1/libraries

Returns all libraries and functions.

Below example fetches all defined libraries.

> GET /functions/v1/libraries
```json
[
  {
    "name": "math",
    "functions":
    [
      {
        "name": "adder",
        "code": "function adder(a, b) { return a + b; }\n function helper() { return; }"
      }
    ]
  },
  {
    "name": "science",
    "functions":
    [
      {
        "name": "f2c",
        "code": "function f2c(f) { return (5/9)*(f-32); }"
      }
    ]
  }
]
```

## Delete a function in a library (available in beta)
> DELETE /functions/v1/libraries/`<library_name>`/functions/`<function_name>`
Deletes the specified function in a library.

Below example deletes function "adder" in the "math" library.
> DELETE /functions/v1/libraries/math/functions/adder

## Delete an entire library (available in beta)
> DELETE /functions/v1/libraries/`<library_name>`
Delete the specified library entirely.

Below example deletes "science" library entirely.
> DELETE /functions/v1/libraries/science

## Delete all libraries
> DELETE /functions/v1/libraries
Deletes all libraries entirely.

Below example deletes all libraries defined in the system.
> DELETE /functions/v1/libraries

## Replacing a function
> PUT /functions/v1/libraries/`<library_name>`/functions/`<function_name>`

This has exactly the same effect as DELETE of URI followed by a POST of the specified body.
That is, the function is deleted if it exists and recreated with the one specified in the body
of this request. It is a functional alias of POST to the same URI.

## Creating/Updating a library
> PUT /functions/v1/libraries/`<function_name>`

This has exactly the same effect as DELETE of URI followed by a POST of the specified body.

That is, if the library exists, it is deleted entirely and replaced with the contents of
the library specified in the body of this call, resulting in the library having only functions
specified by this call exclusively.

## Creating/Updating a collection of libraries
> PUT /functions/v1/libraries

This has exactly the same effect as DELETE of URI followed by a POST of the specified body.

That is, all existing libraries in the system are deleted, and the libraries specified in the
body of this call are created, resulting in the system having exclusively the libraries
specified by this call.

