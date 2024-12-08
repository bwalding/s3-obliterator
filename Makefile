AWS_PROFILE=codeship-prod/infra-admin

all: s3-obliterator

.PHONY: s3-obliterator
s3-obliterator: test.unit
	go build ./s3-obliterator.go

install: test.unit
	go install ./s3-obliterator.go

test: test.unit test.integration

test.unit:
	go test ./...

test.integration:
	@echo "We don't currently differentiate unit vs. integration tests as there are no integration tests"

login: 
	AWS_PROFILE=$(AWS_PROFILE) aws sso login
