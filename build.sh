#!/bin/bash

pushd .
while [ "$top" != "`pwd`" ]; do top="`pwd`"; if [ -d tlm ]; then break; fi; cd ..; done
if [ ! -d "$top/tlm" ]; then echo "Expected to be in build tree"; exit 1; fi
gover="`grep -A20 'SET(GOVERSION ' $top/goproj/src/github.com/couchbase/eventing/CMakeLists.txt  | grep GOVERSION | head -1 | sed 's/^.*\([0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*\).*$/\1/'`"
export GOROOT="$HOME/.cbdepscache/exploded/x86_64/go-$gover/go"
export GOPATH="$top/build/gotmp:$top/goproj:$top/godeps"
export LD_LIBRARY_PATH="$top/install/lib"
export C_INCLUDE_PATH="$top/install/platform/include:$top/install/include:$top/forestdb/include:$top/build/tlm/deps/curl.exploded/include:$top/sigar/include"
export CGO_LDFLAGS="-L $top/install/lib"
export PATH=$PATH:$GOROOT/bin

echo "Building Eventing..."
rm -rf "$top/build/gotmp"
mkdir -p "$top/build/gotmp"
set -e
cd $top/goproj/src/github.com/couchbase/eventing/cmd/producer
go build -ldflags '-s -extldflags "-Wl,-rpath,@executable_path/../lib"' -tags 'enterprise' -o $top/install/bin/eventing-producer
killall eventing-producer || true
cd $top/goproj/src/github.com/couchbase/eventing/cmd/cbevent
go build -ldflags '-s -extldflags "-Wl,-rpath,@executable_path/../lib"' -tags 'enterprise' -o $top/install/bin/cbevent
echo "Done"
popd
