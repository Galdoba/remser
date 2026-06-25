.PHONY: install
install:
	go install ./cmd/...

.PHONY: test
test:
	go test -cover -race ./...
