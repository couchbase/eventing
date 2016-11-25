CXX=g++
CXFLAGS=-std=c++11 -O3 #-Wall

PHOSPHOR_INCLUDE=/var/tmp/repos/phosphor/include/
CBDEPS_DIR=/Users/$(USER)/.cbdepscache/
DYLD_LIBRARY_PATH=/Users/$(USER)/.cbdepscache/lib

LDFLAGS=-luv -ljemalloc -lcouchbase -lhiredis -lcurl -L$(CBDEPS_DIR)lib/ -lv8 -lphosphor

SOURCES=src/client.cc src/commands.cc src/message.cc \
			 src/worker.cc src/bucket.cc src/http_response.cc \
			 src/n1ql.cc src/parse_deployment.cc \
			 src/queue.cc

INCLUDE_DIRS=-I$(CBDEPS_DIR) -I/usr/local/include/hiredis -I inc/ -I$(PHOSPHOR_INCLUDE)

OUT=client.bin

build:
	$(CXX) $(CXFLAGS) $(INCLUDE_DIRS) $(SOURCES) $(LDFLAGS) -o $(OUT)

run:
	go build -race
	./Go2C

clean:
	rm -rf $(OUT)
