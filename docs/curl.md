# Introduction

Curl provides a way of interacting with external entities from event handlers. As the handlers can run on multiple nodes, each multi process and multi thread, it can generate very high load on the REST endpoint being targeted. The incoming load to the handler and the external HTTP endpoint being accessed must be carefully sized to ensure the HTTP endpoint can handle the load.

# Features
The following must be supported:

1. Binding for URL
1. Pooled persistent HTTP connection for each URL binding
1. HTTP verbs GET, POST, HEAD, PUT, DELETE
1. SSL support
1. Basic and Digest Auth
1. Session cookie support
1. Setting headers (like user-agent, content-type)
1. Usernames and passwords as bindings
1. Client certificates as bindings
1. Validating server certificate
1. Ability to exclude weak SSL ciphers


# Pooling
As connection setup is expensive, more so for SSL, a connection pool with a configurable count for each bound URL must be created. The pool should be populated as used. The pooled HTTP connections should use HTTP 1.1 persist mechanism to support idle connections.

# Formats
The endpoint should allow posting JSON, URL encoded form and binary. Binary objects being retreived or posted must be available as JavaScript ArrayBuffer objects.

# Certificates
It should be possible to upload certificates that are sent to identify the node to the HTTP endpoint. It should also be possible to upload a CA certificate and verify the remote against it. Finally, querying public CA should be allowed. The node local certificate should not be available to cURL.


