# This is a shortcut to do incremental eventing builds inside a tree compiled fully from top

top:=$(realpath ../../../../..)
workdir:=$(realpath .)/build
binaries:=eventing-producer eventing-consumer cbevent

goroot := $(HOME)/.cbdepscache/exploded/x86_64/go-1.12.4/go
ccache := $(shell which ccache)
cc_src := $(shell find libs/ v8_consumer/ third_party/ gen/ ../eventing-ee/*.cc -name '*.cc' -o -name '*.c')
go_src := $(shell find . ../eventing-ee/ -name '*.go')
ui_src := $(shell find ./ui/eventing-ui/ -type f)

.PHONY: clean all install test cluster_stop cluster_run functional_test

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
	-I third_party/inspector/ \
	-I libs/query/include \
	-I libs/include/ \
	-I v8_consumer/include/

libs:=\
	-L $(top)/build/tlm/deps/v8.exploded/lib/Release/ \
	-L $(top)/build/tlm/deps/curl.exploded/lib/ \
	-L $(top)/build/tlm/deps/libuv.exploded/lib/ \
	-L $(top)/build/tlm/deps/flatbuffers.exploded/lib/ \
	-L $(top)/build/tlm/deps/openssl.exploded/lib/ \
	-L $(top)/build/tlm/deps/zlib.exploded/lib/ \
	-L $(top)/build/libcouchbase/lib/ \
	-lpthread \
	-lv8 -licui18n -licuuc -lc++  -lv8_for_testing  -lv8_libbase  -lv8_libplatform \
	-lcouchbase \
	-lcurl \
	-luv \
	-lflatbuffers \
	-lssl -lcrypto \
	-lz

tests:= $(shell shuf -e -- \
	handler \
	n1ql \
	curl \
	eventing_reb \
	testrunner_reb \
	kv_reb \
	duplicate_events \
)

goenv:=\
	GOROOT=$(goroot) \
	GOPATH=$(top)/build/go:$(top)/goproj:$(top)/godeps \
	C_INCLUDE_PATH=$(top)/install/platform/include:$(top)/install/include:$(top)/forestdb/include:$(top)/build/tlm/deps/curl.exploded/include:$(top)/sigar/include \
	CGO_LDFLAGS=-L$(top)/install/lib \
	LD_LIBRARY_PATH=$(top)/install/lib \
	PATH=$(goroot)/bin:$(PATH)

goargs:=\
	-v -ldflags '-s -extldflags "-Wl,-rpath,@executable_path/../lib"' -tags 'enterprise'

$(workdir)/cc/eventing/%.o: %.cc
	mkdir -p $(dir $(workdir)/cc/eventing/$<)
	$(ccache) g++ -std=c++1z -c $(includes) -o $@ $<

gen/version/version.cc: util/version.in
	cd $(top) && make -j8

$(workdir): gen/version/version.cc
	mkdir -p $(workdir) $(workdir)/cc $(workdir)/go

$(workdir)/eventing-consumer: $(addprefix $(workdir)/cc/eventing/,$(cc_src:%.cc=%.o))
	$(ccache) g++ $(libs) -o $@ $^

$(workdir)/eventing-producer: $(go_src)
	$(goenv) go build $(goargs) -o $@ cmd/producer/*.go

$(workdir)/cbevent: $(go_src)
	$(goenv) go build $(goargs) -o $@ cmd/cbevent/*.go

$(top)/install/bin/%: $(workdir)/%
	mv -f $@ $@.prior || true
	cp $< $@
	pkill -9 $* || true

$(top)/install/lib/eventing-ui/%: ui/eventing-ui/%
	cp $< $@

clean:
	rm -rf $(workdir)

install: all $(addprefix $(top)/install/bin/,$(binaries)) $(addprefix $(top)/install/lib/,$(ui_src:./ui/%=%))

cluster_stop:
	@while pgrep beam.smp 1>/dev/null; do  \
		pkill -9 'beam.smp|memcached.json|memcached|epmd'; \
		echo "Waiting for cluster to stop"; \
		sleep 5; \
	done
	@pkill -9 epmd || true

cluster_run: cluster_stop $(workdir)
	make -C $(top)/ns_server dataclean
	rm -rf $(top)/ns_server/logs/n_*/*
	cd $(top)/ns_server && LD_LIBRARY_PATH=$(top)/install/lib COUCHBASE_NUM_VBUCKETS=8 ./cluster_run -n4 1>$(workdir)/server.log 2>&1 &
	while ! grep -qs 'Finished compaction for' $(top)/ns_server/logs/n_*/debug.log; do sleep 3; done

functional_test:
	cd tests/functional_tests && $(goenv) $(goroot)/bin/go get -t ./... 1>/dev/null 2>&1
	cd tests/functional_tests && GOMAXPROCS=16 $(goenv) $(goroot)/bin/go test -v -failfast -timeout 24h -tags "$(tests)" | tee $(workdir)/test.log

test: install
	make cluster_run
	make functional_test
	make cluster_stop
