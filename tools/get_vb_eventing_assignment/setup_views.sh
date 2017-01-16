#!/bin/bash

HOST=$1

curl -X PUT -H 'Content-Type: application/json' http://Administrator:asdasd@$HOST:9500/eventing/_design/ddoc1 -d @view_test.json
go build
./get_vb_eventing_assignment $HOST
