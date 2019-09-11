#!/bin/bash

set -e
pushd .

while [ "$top" != "`pwd`" ]; do top="`pwd`"; if [ -d tlm ]; then break; fi; cd ..; done
if [ ! -d "$top/tlm" ]; then echo "Expected to be in build tree"; exit 1; fi

test -d $top/build/gotmp || mkdir -p $top/build/gotmp
test -d $top/build/cctmp || mkdir -p $top/build/cctmp

cd $top/goproj/src/github.com/couchbase/eventing

export gcc="\
  `which ccache` g++ -std=c++1z"

export includes="\
  -I $top/build/tlm/deps/v8.exploded/include/ \
  -I $top/build/tlm/deps/curl.exploded/include/ \
  -I $top/build/tlm/deps/libuv.exploded/include/ \
  -I $top/build/tlm/deps/flatbuffers.exploded/include/ \
  -I $top/build/tlm/deps/openssl.exploded/include/ \
  -I $top/build/tlm/deps/zlib.exploded/include/ \
  -I $top/build/libcouchbase/generated/ \
  -I $top/libcouchbase/include/ \
  -I third_party/inspector/ \
  -I third_party/crc64/ \
  -I third_party/inspector/ \
  -I libs/query/include \
  -I libs/include/ \
  -I v8_consumer/include/"

export libs="\
  -L $top/build/tlm/deps/v8.exploded/lib/Release/ \
  -L $top/build/tlm/deps/curl.exploded/lib/ \
  -L $top/build/tlm/deps/libuv.exploded/lib/ \
  -L $top/build/tlm/deps/flatbuffers.exploded/lib/ \
  -L $top/build/tlm/deps/openssl.exploded/lib/ \
  -L $top/build/tlm/deps/zlib.exploded/lib/ \
  -L $top/build/libcouchbase/lib/ \
  -lpthread \
  -lv8 -licui18n -licuuc -lc++  -lv8_for_testing  -lv8_libbase  -lv8_libplatform \
  -lcouchbase \
  -lcurl \
  -luv \
  -lflatbuffers \
  -lssl -lcrypto \
  -lz"

export srcs="`find \
    libs/query/src/ \
    libs/src/ \
    third_party/inspector/ \
    third_party/crc64/ \
    ../eventing-ee/ \
    gen/parser/ \
    gen/version/ \
    v8_consumer/src/ \
  -maxdepth 1 \
  -name '*.cc'`"

echo "Compiling eventing-consumer"
find $srcs \
  -exec $gcc $includes -c -o $top/build/cctmp/eventing/{}/obj.o {} \;

echo "Linking eventing-consumer"
$gcc $includes $libs \
  -o $top/build/eventing-consumer \
  `find $top/build/cctmp/ -name '*.o'`

gover="`grep -A20 'SET(GOVERSION ' $top/goproj/src/github.com/couchbase/eventing/CMakeLists.txt  | grep GOVERSION | head -1 | sed 's/^.*\([0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*\).*$/\1/'`"
export GOROOT="$HOME/.cbdepscache/exploded/x86_64/go-$gover/go"
export GOPATH="$top/build/gotmp:$top/goproj:$top/godeps"
export C_INCLUDE_PATH="$top/install/platform/include:$top/install/include:$top/forestdb/include:$top/build/tlm/deps/curl.exploded/include:$top/sigar/include"
export CGO_LDFLAGS="-L $top/install/lib"
export LD_LIBRARY_PATH="$top/install/lib"
export PATH=$GOROOT/bin:$PATH

echo "Building eventing-producer"
cd $top/goproj/src/github.com/couchbase/eventing/cmd/producer
go build -ldflags '-s -extldflags "-Wl,-rpath,@executable_path/../lib"' -tags 'enterprise' -o $top/build/eventing-producer

echo "Building cbevent"
cd $top/goproj/src/github.com/couchbase/eventing/cmd/cbevent
go build -ldflags '-s -extldflags "-Wl,-rpath,@executable_path/../lib"' -tags 'enterprise' -o $top/build/cbevent

echo "Updating UI"
cd $top/goproj/src/github.com/couchbase/eventing
cp -urv ui/eventing-ui/. $top/install/lib/eventing-ui/.

for file in eventing-producer eventing-consumer cbevent; do
  cmp --silent $top/build/$file $top/install/bin/$file && continue || true
  killall -9 eventing-producer eventing-consumer cbevent 1>/dev/null 2>/dev/null || true
  cp -uv $top/build/$file $top/install/bin/$file
done

popd > /dev/null
