# This is a shortcut to do incremental eventing builds inside a tree compiled fully from top

top:=$(realpath ../../../../..)
workdir:=$(realpath .)/build
binaries:=eventing-producer eventing-consumer cbevent

goroot := $(shell find ~/.cbdepscache/exploded/x86_64 -type d -name 'go-1.18.1' -print -quit)/go
ccache := $(shell which ccache)
cc_src := $(shell find features/ v8_consumer/ third_party/ gen/ ../eventing-ee/features/ -name '*.cc' -o -name '*.c')
go_src := $(shell find . ../eventing-ee/ -name '*.go')
ui_src := $(shell find ./ui/eventing-ui/ -type f)

.PHONY: clean all install test cluster_stop cluster_start test_deps Test% Suite%

all: $(workdir) $(addprefix $(workdir)/,$(binaries))

includes:=\
	-I $(top)/build/tlm/deps/v8.exploded/include/ \
	-I $(top)/build/tlm/deps/curl.exploded/include/ \
	-I $(top)/build/tlm/deps/libuv.exploded/include/ \
	-I $(top)/build/tlm/deps/flatbuffers.exploded/include/ \
	-I $(top)/build/tlm/deps/openssl.exploded/include/ \
	-I $(top)/build/tlm/deps/zlib.exploded/include/ \
	-I $(top)/build/libcouchbase/generated/ \
	-I $(top)/build/tlm/deps/json.exploded/include/ \
	-I $(top)/libcouchbase/include/ \
	-I third_party/inspector/ \
	-I third_party/crc64/ \
	-I third_party/crc32/ \
	-I third_party/inspector/ \
	-I features/query/include \
	-I features/include/ \
	-I v8_consumer/include/

libs:=\
	-L $(top)/build/tlm/deps/v8.exploded/lib/Release/ \
	-L $(top)/build/tlm/deps/curl.exploded/lib/ \
	-L $(top)/build/tlm/deps/libuv.exploded/lib/ \
	-L $(top)/build/tlm/deps/flatbuffers.exploded/lib/ \
	-L $(top)/build/tlm/deps/openssl.exploded/lib/ \
	-L $(top)/build/tlm/deps/zlib.exploded/lib/ \
	-L $(top)/build/libcouchbase/lib/ \
	-lpthread -lresolv \
	-lv8 -licui18n -licuuc -lc++ -lv8_libbase -lv8_libplatform \
	-lcouchbase \
	-lcurl \
	-luv \
	-lflatbuffers \
	-lssl -lcrypto \
	-lz \
	-lresolv

cflags:=\
	-DENTERPRISE \
	-Wl,-rpath,'@executable_path/../lib' \
	-Wl,-rpath,'$(top)/install/lib'

tests:=\
	handler \
	n1ql \
	curl \
	eventing_reb \
	testrunner_reb \
	kv_reb \
	duplicate_events

goenv:=\
	GOROOT=$(goroot) \
	GOPATH=$(top)/build/go:$(top)/goproj:$(top)/godeps \
	C_INCLUDE_PATH=$(top)/install/platform/include:$(top)/install/include:$(top)/forestdb/include:$(top)/build/tlm/deps/curl.exploded/include:$(top)/sigar/include \
	CGO_LDFLAGS=-L$(top)/install/lib \
	LD_LIBRARY_PATH=$(top)/install/lib \
	PATH=$(goroot)/bin:$(PATH)

goflags:=\
	-v -ldflags '-s -extldflags "-Wl,-rpath,@executable_path/../lib"' -tags 'enterprise neo'

$(workdir)/cc/eventing/%.o: %.cc
	mkdir -p $(dir $(workdir)/cc/eventing/$<)
	$(ccache) g++ -std=c++1z -ggdb -Og -c $(includes) $(cflags) -o $@ $<

gen/version/version.cc: util/version.in
	make realclean
	cd $(top) && make -j8

$(workdir): gen/version/version.cc
	mkdir -p $(workdir) $(workdir)/cc $(workdir)/go

$(workdir)/eventing-consumer: $(addprefix $(workdir)/cc/eventing/,$(cc_src:%.cc=%.o))
	$(ccache) g++ $(libs) $(cflags) -o $@ $^

$(workdir)/eventing-producer: $(go_src)
	$(goenv) go build $(goflags) -o $@ cmd/producer/*.go

$(workdir)/cbevent: $(go_src)
	$(goenv) go build $(goflags) -o $@ cmd/cbevent/*.go

$(top)/install/bin/%: $(workdir)/%
	mv -f $@ $@.prior || true
	cp $< $@
	pkill -9 $* || true

$(top)/install/lib/eventing-ui/%: ui/eventing-ui/%
	cp $< $@

clean:
	rm -rf $(workdir)

realclean: clean
	git clean -dfx gen/
	rm -rf $(top)/build/goproj/src/github.com/couchbase/eventing $(top)/build/goproj/src/github.com/couchbase/eventing-ee

install: all $(addprefix $(top)/install/bin/,$(binaries)) $(addprefix $(top)/install/lib/,$(ui_src:./ui/%=%))

cluster_stop:
	@while pgrep beam.smp 1>/dev/null; do  \
		pkill -9 'beam.smp|memcached.json|memcached|epmd'; \
		echo "Waiting for cluster to stop"; \
		sleep 5; \
	done
	@pkill -9 epmd || true

cluster_start: cluster_stop $(workdir)
	make -C $(top)/ns_server dataclean
	rm -rf $(top)/ns_server/logs/n_*/*
	cd $(top)/ns_server && LD_LIBRARY_PATH=$(top)/install/lib COUCHBASE_NUM_VBUCKETS=8 ./cluster_run -n4 1>$(workdir)/server.log 2>&1 &
	while ! grep -qs 'Finished compaction for' $(top)/ns_server/logs/n_*/debug.log; do sleep 3; done

test_deps:
	cd tests/functional_tests && $(goenv) $(goroot)/bin/go get -t ./...

Test%: install test_deps
	make cluster_start
	cd tests/functional_tests && GOMAXPROCS=16 $(goenv) $(goroot)/bin/go test -v -failfast -timeout 1h -tags "$(tests)" -run $@ | tee $(workdir)/test.log
	make cluster_stop

Suite%: install test_deps
	make cluster_start
	cd tests/functional_tests && GOMAXPROCS=16 $(goenv) $(goroot)/bin/go test -v -failfast -timeout 12h -tags "$(shell echo $* | tr A-Z a-z)" | tee $(workdir)/test.log
	make cluster_stop

test: install test_deps
	make cluster_start
	cd tests/functional_tests && GOMAXPROCS=16 $(goenv) $(goroot)/bin/go test -v -failfast -timeout 24h -tags "$(tests)" | tee $(workdir)/test.log
	make cluster_stop

vet: install test_deps
	$(goenv) $(goroot)/bin/go vet ./...
