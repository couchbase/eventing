#!/bin/bash

if [ "$phase" = "" ]; then phase=unknown; fi
tar -C $WORKSPACE --transform "s/^/logs-$phase-$1-/" -uf $WORKSPACE/logs.tar ns_server/logs 1>/dev/null 2>&1
tar -C $WORKSPACE --transform "s/^/data-$phase-$1-/" -uf $WORKSPACE/logs.tar ns_server/data 1>/dev/null 2>&1
find $WORKSPACE/ns_server/logs -type f -exec sh -c '> {}' \; 2>/dev/null
find $WORKSPACE/ns_server/data -type f -exec sh -c '> {}' \; 2>/dev/null
