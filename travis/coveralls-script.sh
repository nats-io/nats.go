#!/bin/bash -e
echo "mode: count" > acc.out
for Dir in . ./test ./encoders/builtin ./encoders/protobuf
do
    go test -v -covermode=count -coverprofile=profile.out $Dir
    if [ -f ./profile.out ]
    then
	cat profile.out | grep -v "mode: count" >> acc.out
    fi
done
$HOME/gopath/bin/goveralls -coverprofile=acc.out -service=travis-ci
rm -rf ./profile.out
rm -rf ./acc.out
