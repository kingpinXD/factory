.PHONY: build test vet fmt-check install

build:
	go build ./...

test:
	go test -race ./...

vet:
	go vet ./...

fmt-check:
	@out="$$(gofmt -l .)"; if [ -n "$$out" ]; then echo "not gofmt-formatted:"; echo "$$out"; exit 1; fi

install:
	go build -o "$(HOME)/.local/bin/factory" ./cmd/factory
