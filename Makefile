CXX=g++
CXFLAGS=-std=c++11 #-O3 -Wall

PHOSPHOR_INCLUDE=../phosphor/include/
CBDEPS_DIR=/Users/$(USER)/.cbdepscache/
DYLD_LIBRARY_PATH=/Users/$(USER)/.cbdepscache/lib
CMD_DIR=cmd/producer/

LDFLAGS=-luv -ljemalloc -lcouchbase -lhiredis -lcurl -L$(CBDEPS_DIR)lib/ -lv8 -lphosphor

SOURCES=consumer/src/client.cc consumer/src/commands.cc consumer/src/message.cc

INCLUDE_DIRS=-I$(CBDEPS_DIR) -I/usr/local/include/hiredis -I inc/ -I$(PHOSPHOR_INCLUDE)

OUT=$(CMD_DIR)client.bin

build:
	$(CXX) -std=c++11 $(SOURCES) -luv -o $(OUT)
	cd $(CMD_DIR); go build

allopt: build
	$(CXX) $(CXFLAGS) src/client.cc src/commands.cc src/message.cc -luv -O3 -o $(OUT)
	go run main.go

run: build
	$(CMD_DIR)/producer -cfg couchbase:http://cfg-bucket@127.0.0.1:8091 -server http://127.0.0.1:8091

clean:
	rm -rf $(OUT)
