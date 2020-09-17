# REST API
These is an internal REST API, and will change from version to version of the product. All REST API calls must set appropriate Content-Type header,
which is usually `application/json`. The HTTP return code indicates the result, with 2xx codes representing success, 4xx codes indicating a problem
with the request, 5xx indicating internal errors. The last two digits are informational and may change between releases.

## Create a function
>
> `POST /api/v1/functions/<name>`
>

Creates a function using the definition (which includes function settings) sent in the body of the request.
The function name in the definition and the name specified on the API must match and must be unique.
Note that as a function definition includes settings, it is possible to set deploy to true and create
and deploy a function in a single step. It is not recommended to do so however.

## Create several functions
>
> `POST /api/v1/functions`
>

This creates serveral functions as specified in the body of the request. Function names must be unique.
Note that as a function definition includes settings, it is possible to set deploy to true and create
and deploy functions in a single step. It is not recommended to do so however.

## Get a function
>
> `GET /api/v1/functions/<name>`
>

Fetch a function definition. This includes the settings specific to the function. The entire returned
artifact should be treated as an opaque object and should not be edited outside the Couchbase Console UI.

## Get all functions
>
> `GET /api/v1/functions`
>

Fetch all function definitions. This includes the settings specific to each function. The entire returned
artifact should be treated as an opaque object and should not be edited outside the Couchbase Console UI.

## Delete a function
>
> `DELETE /api/v1/functions/<name>`
>

Delete a function. Only functions that are in undeployed state can be deleted. Call expects no body.

## Delete all functions
>
> `DELETE /api/v1/functions`
>

Delete all defined functions. Use with caution. Only functions that are in undeployed state can be deleted.
Call expects no body.

## Get a deployed function's settings
>
> `GET /api/v1/functions/<name>/settings`
> 

Fetch only the settings portion of a **deployed** function. This includes the current deployment status,
pause/resume status, and log level of the function. Some internal settings may be returned for debugging
purposes. (Undeployed function's settings are part of its definition and hence must be examined with functions 
definition editor, and not this endpoint).

## Modify a deployed function's settings
>
> `POST /api/v1/functions/<name>/settings`
>

Edit the settings portion of a **deployed** function. Currently, only the deployment status, pause/resume status
and log level can be changed on a deployed function. Some internal settings can be manipulated for debugging
purposes as well using this endpoint. Settings are merged with existing settings. Hence, only the changed
members must be included in the body of the post. (Undeployed function's settings are part of its definition
and hence must be edited with functions definition editor, and not this endpoint).

## Deploy
Currently, a function is deployed by setting its deployment and processing status to true. This may change in
the future to provide an explicit endpoint to accomplish the same. Note that deployment status and processing
status must always be equal in 5.5 release as we do not yet support pause/resume functionality.

>
> `POST /api/v1/functions/<name>/settings`
>
> {"deployment_status": true, "processing_status": true}
>

## Undeploy
Currently, a function is undeployed by setting its deployment and processing status to false. This may change in
the future to provide an explicit endpoint to accomplish the same. Note that deployment status and processing
status must always be equal in 5.5 release as we do not yet support pause/resume functionality.

>
> `POST /api/v1/functions/<name>/settings`
>
> {"deployment_status": false, "processing_status": false}
>

## Get eventing global config
> 
> `GET /api/v1/config`
>

Config is distinct from settings in the sense of config being global to entire eventing service, while setting is
specific to a particular function definition. Currently, memory quota and metadata bucket are part of the config
that can be fetched using this endpoint. Additional debugging settings may be included.

## Manipulate eventing global config
>
> `POST /api/v1/config`
>

Currently, memory quota and metadata bucket are settable parts of the config using this endpoint. Additional debugging
settings may be included. Config provided is merged, and so unspecified elements retain their prior values. Only modified
values must be included in the body of the call. The response indicates if eventing service needs to be restarted for
the config change to take effect. RAM quota is specified in megabytes.

## Import a list of functions
>
> `POST /api/v1/import`
>

This is a convenience method to import function definitions. Imported functions are always start off in undeployed state
regardless of the setting values of the function definition. The body of the call must contain unmodified function definitions
that were obtained using the `/api/v1/export` call.

## Export a list of functions
>
> `GET /api/v1/export`
> 

This is a convenience method to export all function definitions. Exported functions are always set to undeployed state
at the time of export, regardless of the state in the cluster at time of export. The returned artifact should be treated as an
opaque artifact and must not be edited outside the Couchbase Console UI.

## Get the status of functions
>
> `GET /api/v1/status`
>

This API returns a list of functions and its corresponding `composite_status`. It can have one of the following values - `undeployed`,
`deploying`, `deployed`, `undeploying`.
