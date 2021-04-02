#!/usr/bin/env bash
set -euo pipefail

tag=${1:-}
if [[ -z "${tag}" ]]; then
	echo "missing tag"
	echo "usage: pre-release.sh <tag>"
	exit 1
fi

WC='\033[0;36m'
NC='\033[0m'
echo -e "${WC}=== Creating release branch === ${NC}"

releaseBranch="release/${tag}"
git checkout -b "${releaseBranch}"
mv go.mod go_test.mod
mv go.sum go_test.sum

# Start with empty go.mod file
go mod init

# Build example with the minimal go.mod, this will leave out test dependencies.
# -mod flag instructs to fetch dependencies as needed.
echo -e "${WC}=== Building minimal go.mod === ${NC}"
go build -mod=mod examples/nats-sub/main.go

# Run the tests locally and confirm they pass.
echo -e "${WC}=== Running tests === ${NC}"
sleep 1

# Use readonly to ensure that dependencies are not changed while running tests.
go test ./... -p=1 -v -modfile=go_test.mod -mod=readonly

# Confirm the different in dependencies. go_test.mod should only have test
# dependencies.
modDiff=$(diff go.mod go_test.mod || true)
if [[ -z "${modDiff}" ]]; then
	echo "go.mod and go_test.mod are the same"
	echo "confirm that test dependencies are being left out and try again"
	exit 1
fi

echo
echo -e "${WC}=== diff go.mod go_test.mod === ${NC}"
echo
echo "${modDiff}"
echo
echo
read -e -r -p "Are the test dependencies left out? [y/n] " diffOk
if ! [[ "$diffOk" =~ ^(yes|y)$ ]]; then
	echo "diff not ok, aborting"
	exit 1
fi

echo 
echo -e "${WC}=== Run the following commands to tag the release === ${NC}"
echo 
echo "  git add go.mod go.sum go_test.mod go_test.sum"
echo "  git commit -s -m 'Release ${tag}'"
echo "  git tag -m '${tag}' -a ${tag}"
echo "  git push origin ${releaseBranch} --tags"
echo
echo
