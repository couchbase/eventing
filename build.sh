#!/bin/bash

top="`pwd`/../../../../.."
if [ ! -d "$top/tlm" ]; then echo "Expected eventing as current directory"; exit 1; fi
gover="`grep -A20 'SET(GOVERSION ' CMakeLists.txt  | grep GOVERSION | head -1 | sed 's/^.*\([0-9]\.[0-9].[0-9]\).*$/\1/'`"
export GOROOT="$HOME/.cbdepscache/exploded/x86_64/go-$gover/go"
export GOPATH="$top/build/gotmp:$top/goproj:$top/godeps"
export LD_LIBRARY_PATH="$top/install/lib"
export C_INCLUDE_PATH="$top/install/platform/include:$top/install/include:$top/forestdb/include:$top/install/build/tlm/deps/curl.exploded/include:$top/sigar/include:$top/build/tlm/deps/jemalloc.exploded/include"
export CGO_CFLAGS="-DJEMALLOC=1"
export CGO_LDFLAGS="-L $top/install/lib"
export PATH=$PATH:$GOROOT/bin

echo "Building Eventing..."
rm -rf "$top/build/gotmp"
mkdir -p "$top/build/gotmp"
cd cmd/producer
set -e
go build -ldflags '-extldflags "-Wl,-rpath,@executable_path/../lib"' -tags 'jemalloc enterprise' -o ~/install/bin/eventing-producer
echo "Done"
