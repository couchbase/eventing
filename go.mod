module github.com/couchbase/eventing

go 1.13

replace github.com/couchbase/cbauth => ../cbauth

replace github.com/couchbase/cbft => ../../../../../cbft

replace github.com/couchbase/cbftx => ../../../../../cbftx

replace github.com/couchbase/cbgt => ../../../../../cbgt

replace github.com/couchbase/eventing-ee => ../eventing-ee

replace github.com/couchbase/go-couchbase => ../go-couchbase

replace github.com/couchbase/go_json => ../go_json

replace github.com/couchbase/gomemcached => ../gomemcached

replace github.com/couchbase/indexing => ../indexing

replace github.com/couchbase/n1fty => ../n1fty

replace github.com/couchbase/plasma => ../plasma

replace github.com/couchbase/query => ../query

replace github.com/couchbase/query-ee => ../query-ee

replace github.com/couchbase/eventing => ./empty

replace github.com/couchbase/gocbcore/v9 => github.com/couchbase/gocbcore/v9 v9.0.5

require (
	github.com/couchbase/cbauth v0.0.0-20201026062450-0eaf917092a2
	github.com/couchbase/go-couchbase v0.0.0-20210323165529-9e59ca6b464b
	github.com/couchbase/gocb/v2 v2.1.5
	github.com/couchbase/gocbcore/v9 v9.1.4-0.20210325182448-577aecce6dc6
	github.com/couchbase/gomemcached v0.1.2
	github.com/couchbase/goutils v0.0.0-20210118111533-e33d3ffb5401
	github.com/couchbase/query v0.0.0-20210323234224-e303a18be4a1
	github.com/google/flatbuffers v0.0.0-20200312223339-6df40a247173
	github.com/mitchellh/go-ps v1.0.0
	github.com/pkg/errors v0.9.1
	github.com/santhosh-tekuri/jsonschema v1.2.4
	gopkg.in/couchbase/gocb.v1 v1.6.7
)
