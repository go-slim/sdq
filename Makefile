GO ?= go
GOLANGCI ?= github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.6.2


.PHONY: help
## help: prints this help information
help:
	@echo "\nUsage: \n"
	@echo "  [OPTIONS] make [TARGETS]\n"
	@echo "The targets are:\n"
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ':' |  sed -e 's/^/ /'
	@echo "\nOptions: \n"
	@echo "  GO        set golang binary\n"


.PHONY: test
## test: run all tests excluding examples
test:
	go test -v $(go list ./... | grep -v examples/)


.PHONY: lint
## lint: lint and fix go files
lint:
	@echo "Running golangci-lint with fixes..."
	$(GO) run $(GOLANGCI) run --fix
