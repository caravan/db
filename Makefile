all: test

test: build
	golint ./...
	go vet ./...
	go test ./...

build: generate

generate:
	go generate ./...

deps:
	go get -u golang.org/x/tools/cmd/stringer
	go get -u golang.org/x/lint/golint
