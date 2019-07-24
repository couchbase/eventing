# JS Evaluator REST API

## Creating a library
>
> POST /functions/v1/libraries
>

Content type must be `application/json` <br>
Sample body -
```json
{"name":"math","code":"function add(a, b){ return a + b; }"}
```

## Show all libraries
>
> GET /functions/v1/libraries
>

## Delete a library
>
> DELETE /functions/v1/libraries/<library_name>
>
