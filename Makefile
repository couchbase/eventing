CXX=g++
CXFLAGS=-std=c++11 -O3
LFLAGS=-luv -ljemalloc
SOURCE=src/client.cc src/message.cc
OUT=client.bin

all:
	$(CXX) $(CXFLAGS) $(LFLAGS) $(SOURCE) -o $(OUT)
	go run main.go

clean:
	rm -rf $(OUT)
