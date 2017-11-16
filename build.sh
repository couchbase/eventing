#!/bin/bash

top="`pwd`/../../../../.."
export GOROOT="$HOME/.cbdepscache/exploded/x86_64/go-1.8.3/go"
export GOPATH="$top/build/gotmp:$top/goproj:$top/godeps"
export LD_LIBRARY_PATH="$top/install/lib"
export C_INCLUDE_PATH="$top/install/platform/include:$top/install/include:$top/forestdb/include:$top/install/build/tlm/deps/curl.exploded/include:$top/sigar/include"
export CGO_LDFLAGS="-L $top/install/lib"
export PATH=$PATH:$GOROOT/bin

echo "Building Eventing..."
rm -rf "$top/build/gotmp"
mkdir -p "$top/build/gotmp"
cd cmd/producer
go build -o ~/install/bin/eventing-producer
echo "Done"
