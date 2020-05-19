export GO111MODULE = on

test:
	go test -v ./...

build: go_build

go_build:
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -v ./...

