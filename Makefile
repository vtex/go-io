build_dir = .build
lib = go-io

tests:
	go test `go list ./... | grep -v '^/vendor/'`

build: go_build copy

drone: build

clean:
	rm -rf $(build_dir)/

go_build: clean
	mkdir -p $(build_dir)
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -v -o $(build_dir)/$(lib)

copy:
	mkdir -p $(build_dir)/data
	cp data/* $(build_dir)/data/
	cp Dockerfile $(build_dir)/
