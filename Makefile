.PHONY: build test test-integration cborgen lint

build:
	go build ./...

test:
	go test ./...

test-integration:
	go test -tags integration ./internal/e2e/ -v -timeout 5m

cborgen: ## Regenerate CBOR marshalling for output lexicon types
	go run ./gen
	go build ./...

lint:
	go vet ./...
