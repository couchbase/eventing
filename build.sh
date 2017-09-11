#!/bin/bash

top="`pwd`/../../../../.."
export GOPATH="$top/goproj:$top/godeps"
export LD_LIBRARY_PATH="$top/install/lib"
export C_INCLUDE_PATH="$top/install/platform/include:$top/install/include:$top/forestdb/include:$top/install/build/tlm/deps/curl.exploded/include:$top/sigar/include"
export CGO_LDFLAGS="-L $top/install/lib"


echo "Building Eventing..."
cd cmd/producer
go build -o ~/install/bin/eventing-producer
echo "Done"
