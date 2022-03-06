.PHONY: build
build:
	go build -v ./cmd/consumer

.DEFAULT_GOAL := build