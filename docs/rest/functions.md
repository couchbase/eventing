# REST API
## Function configuration
The function configuration is as shown below -
```json
{
    "appname": "stock-tracker",
    "id": 0,
    "depcfg": {
        "buckets": [],
        "metadata_bucket": "eventing",
        "source_bucket": "default"
    },
    "appcode": "function OnUpdate(doc, meta) {\n    log('document', doc);\n}\nfunction OnDelete(meta) {\n}",
    "settings": {
        "app_log_max_files": 10,
        "app_log_max_size": 10485760,
        "checkpoint_interval": 10000,
        "cleanup_timers": false,
        "cpp_worker_thread_count": 2,
        "curl_timeout": 500,
        "dcp_stream_boundary": "everything",
        "deadline_timeout": 3,
        "deployment_status": false,
        "description": "",
        "enable_recursive_mutation": false,
        "execution_timeout": 1,
        "lcb_inst_capacity": 5,
        "log_level": "TRACE",
        "memory_quota": 256,
        "processing_status": false,
        "rbacpass": "asdasd",
        "rbacrole": "admin",
        "rbacuser": "eventing",
        "skip_timer_threshold": 86400,
        "sock_batch_size": 1,
        "tick_duration": 5000,
        "timer_processing_tick_interval": 500,
        "timer_worker_pool_size": 3,
        "vb_ownership_giveup_routine_count": 3,
        "vb_ownership_takeover_routine_count": 3,
        "worker_count": 3,
        "worker_queue_cap": 1000000,
        "xattr_doc_timer_entry_prune_threshold": 100
    }
}
```

## Create a function
`POST` `/api/v1/functions/<name>`

Body must contain the function configuration with `Content-type:application/json`

> The name of the function and the `appname` in function configuration must match

### Create a list of functions
`POST` `/api/v1/functions`

Body must contain a list of function configurations with `Content-type:application/json`

--
## Get the function
`GET` `/api/v1/functions/<name>`

### Get a list of functions
`GET` `/api/v1/functions`

--
## Delete the function
`DELETE` `/api/v1/functions/<name>`

### Delete all functions
`DELETE` `/api/v1/functions`

--
## Modify a function's settings
`POST` `/api/v1/functions/<name>/settings`

Body must contain key-value of pairs of the desired settings with `Content-type:application/json`
