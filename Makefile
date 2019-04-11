tests:
	go test `go list ./... | grep -v '^/vendor/'`

build: go_build

drone: build

go_build:
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -v ./...

