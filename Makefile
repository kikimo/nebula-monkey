.PHONY: build test fmt

default: build

build: fmt
	go mod tidy
	go build

test:
	go mod tidy
	go test -v -race

fmt:
	go fmt
