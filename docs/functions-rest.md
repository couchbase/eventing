# REST API
In 5.1, responses returned by below functions are opaque and should not be edited outside of eventing UI. In a future release, the returned data format will be standardized and editing allowed. All REST API calls must set appropriate Content-Type header, which is usually `application/json`

## Create a function
`POST` `/api/v1/functions/<name>`
> Function name in body must match function name on URL. Function definition includes its current settings.

## Create several functions
`POST` `/api/v1/functions`

## Get a function
`GET` `/api/v1/functions/<name>`
> Function definition includes its settings

## Get all functions
`GET` `/api/v1/functions`

## Delete a function
`DELETE` `/api/v1/functions/<name>`

## Delete all functions
`DELETE` `/api/v1/functions`

## Manipulate a function's settings
`GET` `/api/v1/functions/<name>/settings`

## Modify a function's settings
`POST` `/api/v1/functions/<name>/settings`
> Settings provided are merged, and so unspecified elements retain their prior values.

## Get eventing global config
`GET` `/api/v1/config`

## Manipulate eventing global config
`POST` `/api/v1/config`
> Config provided is merged, and so unspecified elements retain their prior values.

```json
{
 "ram_quota": 4,
 "metadata_bucket": "metadata"
}
```
Response
```json
{
 "restart": false
}
```
> 1. The unit for ram_quota is MB.
> 2. The response for the POST request indicates whether the eventing process must be restarted for changes to take effect.