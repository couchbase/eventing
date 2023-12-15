module github.com/couchbase/eventing

go 1.21

replace golang.org/x/text => golang.org/x/text v0.4.0

replace github.com/couchbase/cbauth => ../cbauth

replace github.com/couchbase/cbft => ../../../../../cbft

replace github.com/couchbase/cbftx => ../../../../../cbftx

replace github.com/couchbase/hebrew => ../../../../../hebrew

replace github.com/couchbase/cbgt => ../../../../../cbgt

replace github.com/couchbase/eventing-ee => ../eventing-ee

replace github.com/couchbase/go-couchbase => ../go-couchbase

replace github.com/couchbase/go_json => ../go_json

replace github.com/couchbase/gomemcached => ../gomemcached

replace github.com/couchbase/goutils => ../goutils

replace github.com/couchbase/godbc => ../godbc

replace github.com/couchbase/indexing => ../indexing

replace github.com/couchbase/gometa => ../gometa

replace github.com/couchbase/n1fty => ../n1fty

replace github.com/couchbase/plasma => ../plasma

replace github.com/couchbase/query => ../query

replace github.com/couchbase/query-ee => ../query-ee

replace github.com/couchbase/regulator => ../regulator

require (
    github.com/couchbase/cbauth v0.1.11
    github.com/couchbase/go-couchbase v0.1.1
    github.com/couchbase/gocb/v2 v2.7.0
    github.com/couchbase/gocbcore/v9 v9.1.10
    github.com/couchbase/gomemcached v0.2.2-0.20230407174933-7d7ce13da8cc
    github.com/couchbase/goutils v0.1.2
    github.com/couchbase/query v0.0.0-20231201224521-b47444ea33a9
    github.com/google/flatbuffers v23.5.26+incompatible
    github.com/mitchellh/go-ps v1.0.0
    github.com/pkg/errors v0.9.1
    github.com/santhosh-tekuri/jsonschema v1.2.4
)

require (
    github.com/beorn7/perks v1.0.1 // indirect
    github.com/cespare/xxhash/v2 v2.2.0 // indirect
    github.com/couchbase/clog v0.1.0 // indirect
    github.com/couchbase/go_json v0.0.0-20220330123059-4473a21887c8 // indirect
    github.com/couchbase/gocbcore/v10 v10.3.0 // indirect
    github.com/couchbase/gocbcoreps v0.1.0 // indirect
    github.com/couchbase/goprotostellar v1.0.0 // indirect
    github.com/couchbase/regulator v0.0.0-00010101000000-000000000000 // indirect
    github.com/couchbaselabs/gocbconnstr/v2 v2.0.0-20230515165046-68b522a21131 // indirect
    github.com/golang/protobuf v1.5.3 // indirect
    github.com/golang/snappy v0.0.4 // indirect
    github.com/google/uuid v1.3.1 // indirect
    github.com/gorilla/mux v1.8.0 // indirect
    github.com/grpc-ecosystem/go-grpc-middleware v1.4.0 // indirect
    github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
    github.com/prometheus/client_golang v1.13.0 // indirect
    github.com/prometheus/client_model v0.2.0 // indirect
    github.com/prometheus/common v0.37.0 // indirect
    github.com/prometheus/procfs v0.8.0 // indirect
    github.com/youmark/pkcs8 v0.0.0-20201027041543-1326539a0a0a // indirect
    go.uber.org/multierr v1.11.0 // indirect
    go.uber.org/zap v1.26.0 // indirect
    golang.org/x/crypto v0.15.0 // indirect
    golang.org/x/net v0.17.0 // indirect
    golang.org/x/sys v0.14.0 // indirect
    golang.org/x/text v0.14.0 // indirect
    google.golang.org/genproto/googleapis/rpc v0.0.0-20231016165738-49dd2c1f3d0b // indirect
    google.golang.org/grpc v1.59.0 // indirect
    google.golang.org/protobuf v1.31.0 // indirect
)
