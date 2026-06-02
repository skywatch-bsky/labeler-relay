.PHONY: build test cborgen lint

build:
	go build ./...

test:
	go test ./...

cborgen: ## Regenerate CBOR marshalling for output lexicon types
	go run ./gen
	go build ./...

lint:
	go vet ./...
