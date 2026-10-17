GO ?= go

.PHONY: build test vet fmt local kind-up kind-smoke
build:
	mkdir -p bin
	CGO_ENABLED=0 $(GO) build -trimpath -o bin/gfs ./cmd/gfs

test:
	$(GO) test -race -count=1 ./...

vet:
	$(GO) vet ./...

fmt:
	gofmt -w cmd internal

local: build
	./scripts/local.sh

kind-up:
	./scripts/kind-up.sh

kind-smoke:
	./scripts/kind-smoke.sh
