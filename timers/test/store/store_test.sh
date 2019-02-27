#!/bin/bash

pushd .
while [ "$top" != "`pwd`" ]; do top="`pwd`"; if [ -d tlm ]; then break; fi; cd ..; done
if [ ! -d "$top/tlm" ]; then echo "Expected to be in build tree"; exit 1; fi
gover="`grep -A20 'SET(GOVERSION ' $top/goproj/src/github.com/couchbase/eventing/CMakeLists.txt  | grep GOVERSION | head -1 | sed 's/^.*\([0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*\).*$/\1/'`"
export GOROOT="$HOME/.cbdepscache/exploded/x86_64/go-$gover/go"
export GOPATH="$top/build/gotmp:$top/goproj:$top/godeps"
export LD_LIBRARY_PATH="$top/install/lib"
export C_INCLUDE_PATH="$top/install/platform/include:$top/install/include:$top/forestdb/include:$top/install/build/tlm/deps/curl.exploded/include:$top/sigar/include"
export CGO_LDFLAGS="-L $top/install/lib"
export PATH=$PATH:$GOROOT/bin

set -e
cd $top/goproj/src/github.com/couchbase/eventing/timers/test/store
go install -ldflags "-s -extldflags '-Wl,-rpath,@executable_path/../lib'" -tags 'enterprise'
go build -ldflags "-s -extldflags '-Wl,-rpath,@executable_path/../lib'" -tags 'enterprise' storetest.go

$top/install/bin/couchbase-cli bucket-flush -c 127.0.0.1 -u Administrator -p asdasd --bucket default --force
echo "CREATE PRIMARY INDEX ON default;" | $top/install/bin/cbq -u Administrator -p asdasd -e http://localhost:8091 | grep "msg"

sleep 5
./storetest $*
rm storetest

echo 'SELECT COUNT(*) as count FROM default WHERE meta().id LIKE "timertest:%" AND meta().id NOT LIKE "timertest:tm:%:sp";' | \
  $top/install/bin/cbq -u Administrator -p asdasd -e http://localhost:8091 | \
    grep '"count":' | grep -v number | grep -qs '"count": 0' || \
      echo "Failed: Junk data found"

echo 'SELECT COUNT(*) as count FROM default WHERE meta().id LIKE "timertest:tm:%:sp" AND sta != stp;' | \
  $top/install/bin/cbq -u Administrator -p asdasd -e http://localhost:8091 | \
    grep '"count":' | grep -v number | grep -qs '"count": 0' || \
      echo "Failed: Mismatched span start/stop found"

popd

