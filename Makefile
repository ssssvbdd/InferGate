.PHONY: build test clean

build:
	mkdir -p bin
	cd services/gateway && go build -trimpath -o ../../bin/infergate ./cmd/server

test:
	cd services/gateway && go test ./...
	cd services/optimizer && python3 -m unittest -v
	cd tools/perf-lab && python3 -m unittest discover -s tests -v

clean:
	rm -rf bin
