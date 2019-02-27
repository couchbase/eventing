#!/usr/bin/env bash

set -e
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

COUCHBASE_CLI_PATH=$top/install/bin
POPULATE_DATA_PATH=$top/goproj/src/github.com/couchbase/eventing/tools/populate_data

cd $top/goproj/src/github.com/couchbase/eventing/tools/populate_data
go build

CLUSTER_IP=127.0.0.1:9000

USERNAME=Administrator
PASSWORD=asdasd

BUCKET_1=source
BUCKET_2=destination
METADATA_BUCKET=metadata

BUCKET_1_SIZE=100
BUCKET_2_SIZE=100
METADATA_BUCKET_SIZE=100

FUNCTION_PATH=$top/goproj/src/github.com/couchbase/eventing/timers/test/full/timers-new-api.json
FUNCTION_NAME="timers-new-api"

ITEM_COUNT=10000

FUNCTION_CONTENT=`cat ${FUNCTION_PATH}`
cd ${COUCHBASE_CLI_PATH}

# Cluster initialization
./couchbase-cli cluster-init \
	-c  ${CLUSTER_IP} \
	--cluster-username ${USERNAME} --cluster-password ${PASSWORD} \
	--services=data,query,index,eventing || true

# Create buckets or flush them
./couchbase-cli bucket-create \
	-c ${CLUSTER_IP} \
	-u ${USERNAME} -p ${PASSWORD} \
	--bucket ${BUCKET_1} --bucket-type couchbase --bucket-ramsize ${BUCKET_1_SIZE} --enable-flush 1 \
|| ./couchbase-cli bucket-flush \
	-c ${CLUSTER_IP} \
	-u ${USERNAME} -p ${PASSWORD} \
	--bucket ${BUCKET_1} --force

./couchbase-cli bucket-create \
	-c ${CLUSTER_IP} \
	-u ${USERNAME} -p ${PASSWORD} \
	--bucket ${METADATA_BUCKET} --bucket-type couchbase --bucket-ramsize ${METADATA_BUCKET_SIZE} --enable-flush 1 \
|| ./couchbase-cli bucket-flush \
	-c ${CLUSTER_IP} \
	-u ${USERNAME} -p ${PASSWORD} \
	--bucket ${METADATA_BUCKET} --force

./couchbase-cli bucket-create \
	-c ${CLUSTER_IP} \
	-u ${USERNAME} -p ${PASSWORD} \
	--bucket ${BUCKET_2} --bucket-type couchbase --bucket-ramsize ${BUCKET_2_SIZE} --enable-flush 1 \
|| ./couchbase-cli bucket-flush \
	-c ${CLUSTER_IP} \
	-u ${USERNAME} -p ${PASSWORD} \
	--bucket ${BUCKET_2} --force

sleep 5
set +e

while true; do
  echo "Waiting for N1QL"
  sleep 3
  echo "SELECT 1 AS one;"| ./cbq -u $USERNAME -p $PASSWORD -e http://$CLUSTER_IP 2>&1 | grep -qs '"one": 1' && break
done

echo "CREATE PRIMARY INDEX ON $BUCKET_2;" | ./cbq -u $USERNAME -p $PASSWORD -e http://$CLUSTER_IP
curl -s -POST -d "${FUNCTION_CONTENT}" "http://${USERNAME}:${PASSWORD}@${CLUSTER_IP}/_p/event/api/v1/functions/${FUNCTION_NAME}" | \
 jq '.' | tee /dev/stderr | grep -qs 'ERR_APP_ALREADY_DEPLOYED' || sleep 20

${POPULATE_DATA_PATH}/populate_data -bucket source -count ${ITEM_COUNT} -doc credit_score -user $USERNAME -pass $PASSWORD http://${CLUSTER_IP} http://${CLUSTER_IP}

status=Failed
echo "Now counting timers fired:"
for i in {1..60}; do
  echo -n "At time $i "
   echo "SELECT COUNT(*) as count FROM destination;" | \
     ./cbq -p no -u Administrator -p asdasd -e http://$CLUSTER_IP | grep count | grep -v number | \
       tee /dev/stderr | grep -qs "\"count\": $ITEM_COUNT" && status=Success && break
  sleep 1
done

echo "$status!"

popd
