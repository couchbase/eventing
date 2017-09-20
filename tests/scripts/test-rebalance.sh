#!/bin/bash

SOURCE_DIR=$1
EVENTING_DIR="${SOURCE_DIR}goproj/src/github.com/couchbase/eventing"

REBALANCE_SLEEP_DUR=60
SERVER_LIST="127.0.0.1:9001 127.0.0.1:9002 127.0.0.1:9003"
BOOTSTRAP_NODE="127.0.0.1:9000"

echo "Starting up cluster of 4 nodes using cluster_run"
cd $SOURCE_DIR/ns_server; make dataclean; ./cluster_run -n4 &>/dev/null &
sleep 15;

echo "Initialising cluster of one node"
cd $SOURCE_DIR/ns_server; ./cluster_connect -n1 -s 500 -I 256 -r 2
sleep 15

echo "Storing app definition in metakv"
cd $EVENTING_DIR/tools/metakv/; go build -race; export CBAUTH_REVRPC_URL="http://Administrator:asdasd@$BOOTSTRAP_NODE/_cbauth"; ./metakv ../../cmd/producer/apps/credit_score

echo "Waiting for current eventing node to spawn all dcp streams, sleep for" $REBALANCE_SLEEP_DUR "s"
sleep $REBALANCE_SLEEP_DUR

echo "Populating 100k docs in loop using cbworkloadgen"
while true;
do
    $SOURCE_DIR/install/bin/cbworkloadgen -n $BOOTSTRAP_NODE -u Administrator -p asdasd -j -i 10000 &> /dev/null;
    sleep 2;
done &

# Eventing server-add
for eventingNode in $SERVER_LIST
do
    echo "Adding eventing node " $eventingNode " in the existing cluster"
    $SOURCE_DIR/install/bin/couchbase-cli server-add -c $BOOTSTRAP_NODE \
        -u Administrator -p asdasd --server-add-username=Administrator \
        --server-add-password=asdasd --services=eventing --server-add=$eventingNode

    sleep 10
    echo "Triggering rebalance post server-add in the cluster"

    $SOURCE_DIR/install/bin/couchbase-cli rebalance -c $BOOTSTRAP_NODE \
        -u Administrator -p asdasd
done

echo "Capturing state of vbucket metadata post eventing rebalance"
cd $EVENTING_DIR/tools/get_vb_eventing_assignment/; ./setup_views.sh 127.0.0.1 > event_reb.metadata.log

#Eventing failover
for eventingNode in $SERVER_LIST
do
    echo "Failing over eventing node " $eventingNode " from existing cluster"
    $SOURCE_DIR/install/bin/couchbase-cli failover -c $BOOTSTRAP_NODE \
        -u Administrator -p asdasd --force --server-failover=$eventingNode

    sleep 10
    echo "Triggering rebalance post eventing hard failover in the cluster"

    $SOURCE_DIR/install/bin/couchbase-cli rebalance -c $BOOTSTRAP_NODE \
        -u Administrator -p asdasd
done

echo "Capturing state of vbucket metadata post eventing failover"
cd $EVENTING_DIR/tools/get_vb_eventing_assignment/; ./setup_views.sh 127.0.0.1 > event_failover.metadata.log

#eventing + kv server-add
for eKvNode in $SERVER_LIST
do
    echo "Adding eventing+kv node " $eKvNode " in the existing cluster"
    $SOURCE_DIR/install/bin/couchbase-cli server-add -c $BOOTSTRAP_NODE \
        -u Administrator -p asdasd --server-add-username=Administrator \
        --server-add-password=asdasd --services=data,eventing --server-add=$eKvNode

    sleep 10
    echo "Triggering rebalance post server-add in the cluster"

    $SOURCE_DIR/install/bin/couchbase-cli rebalance -c $BOOTSTRAP_NODE \
        -u Administrator -p asdasd
done

echo "Capturing state of vbucket metadata post eventing + kv rebalance"
cd $EVENTING_DIR/tools/get_vb_eventing_assignment/; ./setup_views.sh 127.0.0.1 > event_kv_reb.metadata.log

#eventing + kv failover
for eKvNode in $SERVER_LIST
do
    echo "Failing over eventing + kv node " $eKvNode " from existing cluster"
    $SOURCE_DIR/install/bin/couchbase-cli failover -c $BOOTSTRAP_NODE \
        -u Administrator -p asdasd --server-failover=$eKvNode

    sleep 10
    echo "Triggering rebalance post eventing + kv graceful failover in the cluster"

    $SOURCE_DIR/install/bin/couchbase-cli rebalance -c $BOOTSTRAP_NODE \
        -u Administrator -p asdasd
done

echo "Capturing state of vbucket metadata post eventing + kv failover"
cd $EVENTING_DIR/tools/get_vb_eventing_assignment/; ./setup_views.sh 127.0.0.1 > event_kv_failover.metadata.log

echo "Taking goroutine dump"
curl http://127.0.0.1:25000/debug/pprof/goroutine?debug=1 > goroutine_dump.log

echo "Performing cleanup by killing up spawned cluster_run instance"
kill -9 `ps aux | grep cluster_run | grep -v grep | awk '{print $2}'`
