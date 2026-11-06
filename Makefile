BINARY_NAME := wex-fastest-downloader
LIB_NAME := libwexdl.so

.PHONY: all build build-shared clean

all: build

build:
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o $(BINARY_NAME) .

build-shared:
	CGO_ENABLED=1 go build -trimpath -ldflags="-s -w" -buildmode=c-shared -o $(LIB_NAME) .

clean:
	rm -f $(BINARY_NAME) $(LIB_NAME) $(LIB_NAME:%.so=%.h) *.part *.wexdl
