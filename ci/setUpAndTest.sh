#!/bin/bash

function checkProgress() {
    progress=0.0
    complete=1.0

    while (( $(echo "$complete > $progress" |bc -l) ))
    do
        echo $progress
        progress=( $(curl http://127.0.0.1:25000/getAggRebalanceProgress -s) )
        echo $progress
    done
}

SOURCE_DIR=$1
EVENTING_DIR=$2

REBALANCE_SLEEP_DUR=30

echo "Starting up cluster of 4 nodes using cluster_run"
cd $SOURCE_DIR/ns_server; make dataclean; ./cluster_run -n4 &>/dev/null &
sleep 15;

echo "Initialising cluster of one node"
cd $SOURCE_DIR/ns_server; ./cluster_connect -n1 -s 500 -I 256
sleep 15

echo "Storing app definition in metakv"
cd $EVENTING_DIR/tools/metakv/; go build -race; export CBAUTH_REVRPC_URL="http://Administrator:asdasd@127.0.0.1:9000/_cbauth"; ./metakv ../../cmd/producer/apps/credit_score

echo "Waiting for current eventing node to spawn all dcp streams, sleep from 60s"
sleep $REBALANCE_SLEEP_DUR
# checkProgress

echo "Populating 100k docs in the bucket"
$SOURCE_DIR/install/bin/cbworkloadgen -i 100000 -b default \
    --prefix="pqrs" -j -n localhost:9000 -l &

# Eventing rebalance
for eventingNode in 127.0.0.1:9001 127.0.0.1:9002 127.0.0.1:9003
do
    echo "Adding eventing node " $eventingNode " in the existing cluster"
    $SOURCE_DIR/install/bin/couchbase-cli server-add -c 127.0.0.1:9000 \
        -u Administrator -p asdasd --server-add-username=Administrator \
        --server-add-password=asdasd --services=eventing --server-add=$eventingNode

    sleep 10
    echo "Triggering rebalance post server-add in the cluster"

    $SOURCE_DIR/install/bin/couchbase-cli rebalance -c 127.0.0.1:9000 \
        -u Administrator -p asdasd

    echo "Waiting for rebalance to finish, sleep for " $REBALANCE_SLEEP_DUR "s"
    sleep $REBALANCE_SLEEP_DUR
    #checkProgress
done

echo "Capturing state of vbucket metadata post eventing rebalance"
cd $EVENTING_DIR/tools/get_vb_eventing_assignment/; ./setup_views.sh 127.0.0.1 > event_reb.metadata.log

#Eventing failover
for eventingNode in 127.0.0.1:9001 127.0.0.1:9002 127.0.0.1:9003
do
    echo "Failing over eventing node " $eventingNode " from existing cluster"
    $SOURCE_DIR/install/bin/couchbase-cli failover -c 127.0.0.1:9000 \
        -u Administrator -p asdasd --force --server-failover=$eventingNode

    sleep 10
    echo "Triggering rebalance post eventing hard failover in the cluster"

    $SOURCE_DIR/install/bin/couchbase-cli rebalance -c 127.0.0.1:9000 \
        -u Administrator -p asdasd

    echo "Waiting for rebalance to finish, sleep for " $REBALANCE_SLEEP_DUR "s"
    sleep $REBALANCE_SLEEP_DUR
done

echo "Capturing state of vbucket metadata post eventing failover"
cd $EVENTING_DIR/tools/get_vb_eventing_assignment/; ./setup_views.sh 127.0.0.1 > event_failover.metadata.log

for eKvNode in 127.0.0.1:9001 127.0.0.1:9002 127.0.0.1:9003
do
    echo "Adding eventing+kv node " $eKvNode " in the existing cluster"
    $SOURCE_DIR/install/bin/couchbase-cli server-add -c 127.0.0.1:9000 \
        -u Administrator -p asdasd --server-add-username=Administrator \
        --server-add-password=asdasd --services=data,eventing --server-add=$eKvNode

    sleep 10
    echo "Triggering rebalance post server-add in the cluster"

    $SOURCE_DIR/install/bin/couchbase-cli rebalance -c 127.0.0.1:9000 \
        -u Administrator -p asdasd

    echo "Waiting for rebalance to finish, sleep for " $REBALANCE_SLEEP_DUR "s"
    sleep $REBALANCE_SLEEP_DUR
    #checkProgress
done

echo "Capturing state of vbucket metadata post eventing + kv rebalance"
cd $EVENTING_DIR/tools/get_vb_eventing_assignment/; ./setup_views.sh 127.0.0.1 > event_kv_reb.metadata.log

echo "Taking goroutine dump"
curl http://127.0.0.1:25000/debug/pprof/goroutine?debug=1 > goroutine_dump.log

echo "Performing cleanup by killing up spawned cluster_run instance"
kill -9 `ps aux | grep cluster_run | grep -v grep | awk '{print $2}'`

