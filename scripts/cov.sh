#!/bin/bash -e
# Run from the repo root: ./scripts/cov.sh [CI]
#
# Requires TESTER_NATS_URL to point at a running synadia/server-tester
# instance (see `make tester-up` or `make tester-up-host`). The integration
# tests in ./test, ./jetstream/test, ./micro/test all skip otherwise.

if [ -z "$TESTER_NATS_URL" ]; then
    echo "TESTER_NATS_URL must be set (e.g. nats://localhost:4222 after 'make tester-up-host')." >&2
    exit 1
fi

rm -rf ./cov
mkdir cov

# White-box tests at the repo root (skip_no_race_tests excludes the NoRace
# norace_test.go that lives in ./test and we cover separately below).
go test -modfile=go_test.mod --failfast -vet=off -v -covermode=atomic -coverprofile=./cov/nats.out . -tags=skip_no_race_tests

# Integration tests against the core nats.go package.
go test -modfile=go_test.mod --failfast -vet=off -v -covermode=atomic -coverprofile=./cov/test.out -coverpkg=github.com/nats-io/nats.go ./test -tags=skip_no_race_tests,internal_testing

# Integration tests against the new jetstream package.
go test -modfile=go_test.mod --failfast -vet=off -v -covermode=atomic -coverprofile=./cov/jetstream.out -coverpkg=github.com/nats-io/nats.go/jetstream ./jetstream/...

# Integration tests against the micro package.
go test -modfile=go_test.mod --failfast -vet=off -v -covermode=atomic -coverprofile=./cov/service.out -coverpkg=github.com/nats-io/nats.go/micro ./micro/...

# Encoders (only their dedicated tests in ./test).
go test -modfile=go_test.mod --failfast -vet=off -v -covermode=atomic -coverprofile=./cov/builtin.out -coverpkg=github.com/nats-io/nats.go/encoders/builtin ./test -run 'TestEncBuiltin|TestEncodedConn' -tags=skip_no_race_tests
go test -modfile=go_test.mod --failfast -vet=off -v -covermode=atomic -coverprofile=./cov/protobuf.out -coverpkg=github.com/nats-io/nats.go/encoders/protobuf ./test -run 'TestEncProto' -tags=skip_no_race_tests

gocovmerge ./cov/*.out > acc.out
rm -rf ./cov

# Without argument, launch browser results. We are going to push to coveralls only
# from ci.yml and after success of the build (and result of pushing will not affect
# build result).
if [[ $1 == "" ]]; then
    go tool cover -html=acc.out
fi
