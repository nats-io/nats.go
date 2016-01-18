#!/bin/bash -e
# Run from directory above via ./scripts/cov.sh

rm -rf ./cov
mkdir cov
go test -v -covermode=atomic -coverprofile=./cov/nats.out
go test -v -covermode=atomic -coverprofile=./cov/builtin.out ./encoders/builtin
go test -v -covermode=atomic -coverprofile=./cov/protobuf.out ./encoders/protobuf
go test -v -covermode=atomic -coverprofile=./cov/test.out -coverpkg=github.com/nats-io/nats ./test
gocovmerge ./cov/*.out > acc.out
rm -rf ./cov

# If we have an arg, assume travis run and push to coveralls. Otherwise launch browser results
if [[ -n $1 ]]; then
    $HOME/gopath/bin/goveralls -coverprofile=acc.out -service travis-ci
    rm -rf ./acc.out
else
    go tool cover -html=acc.out
fi
