set -e
go run *.go
~/Couch/install/bin/cbc-bucket-flush -u Administrator -P asdasd --spec='couchbase://localhost/register'
~/Couch/install/bin/cbimport json -c http://localhost:8091 -u Administrator -p asdasd -b register -f list -g '%type%:%txnid%' -d file://txns.json 
~/Couch/install/bin/cbimport json -c http://localhost:8091 -u Administrator -p asdasd -b register -f list -g '%type%:%cardnumber%' -d file://cards.json
~/Couch/install/bin/cbimport json -c http://localhost:8091 -u Administrator -p asdasd -b register -f list -g '%type%:%merchantid%' -d file://merchants.json
