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
## test: run all tests excluding examples and benchmarks
test:
	$(GO) test -v $$($(GO) list ./... | grep -v -E '(examples|benchmarks)')


.PHONY: lint
## lint: lint and fix go files
lint:
	@echo "Running golangci-lint with fixes..."
	$(GO) run $(GOLANGCI) run --fix


.PHONY: bench
## bench: run benchmark tests
bench:
	$(GO) test -bench=. -benchmem ./benchmarks/


.PHONY: stability
## stability: run stability tests (use DURATION=1h, JOBS=1000000, TOPICS=10000 to customize)
stability:
	STABILITY_DURATION=$(or $(DURATION),5m) \
	JOB_COUNT=$(or $(JOBS),100000) \
	TOPIC_COUNT=$(or $(TOPICS),1000) \
	$(GO) test -run=TestStability -v -timeout=26h


.PHONY: stability-report
## stability-report: run stability tests and generate JSON/HTML report (use REPORT=path/to/report.json)
stability-report:
	@echo "Running stability tests with report generation..."
	@echo "  DURATION=$(or $(DURATION),30s) JOBS=$(or $(JOBS),50000) TOPICS=$(or $(TOPICS),500)"
	@echo "  Report: $(or $(REPORT),stability_report.json)"
	STABILITY_DURATION=$(or $(DURATION),30s) \
	JOB_COUNT=$(or $(JOBS),50000) \
	TOPIC_COUNT=$(or $(TOPICS),500) \
	STABILITY_REPORT=$(or $(REPORT),stability_report.json) \
	$(GO) test -run=TestStability_Report -v -timeout=26h


.PHONY: stability-24h
## stability-24h: run 24-hour stability tests with full report
stability-24h:
	@echo "============================================================"
	@echo "  SDQ 24-Hour Stability Test"
	@echo "============================================================"
	@echo "  Total Duration: ~24 hours"
	@echo "    - LongRunning: ~19.2h (80%)"
	@echo "    - Other tests: ~4.8h (20%)"
	@echo "  Jobs: 1,000,000"
	@echo "  Topics: 10,000"
	@echo "  Report: stability_24h_$$(date +%Y%m%d_%H%M%S).json"
	@echo "  Started at: $$(date)"
	@echo "============================================================"
	STABILITY_DURATION=24h \
	JOB_COUNT=1000000 \
	TOPIC_COUNT=10000 \
	STABILITY_REPORT=stability_24h_$$(date +%Y%m%d_%H%M%S).json \
	$(GO) test -run=TestStability_Report -v -timeout=26h
