CC=gcc
SOURCE=src/client.c
OUT=client

all:
	$(CC) -luv $(SOURCE) -o $(OUT)
	go run main.go

clean:
	rm -rf $(OUT)
