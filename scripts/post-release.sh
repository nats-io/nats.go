#!/usr/bin/env bash
set -euo pipefail

tag=${1:-}
if [[ -z "${tag}" ]]; then
	echo "missing tag"
	echo "usage: post-release.sh <tag>"
	exit 1
fi

WC='\033[0;36m'
NC='\033[0m'
echo -e "${WC}=== Cleaning up === ${NC}"

# Return master to development mode once again.
mv go_test.mod go.mod
mv go_test.sum go.sum

go test ./... -p=1 -v

echo 
echo -e "${WC}=== Run the following commands to finish the release === ${NC}"
echo 
echo "  git add go.mod go.sum"
echo "  git rm go_test.mod go_test.sum"
echo "  git commit -s -m 'Post Release ${tag} steps'"
echo "  git checkout master"
echo "  git merge --no-ff 'release/${tag}'"
echo "  git push origin master"
echo
echo "  # Delete release branch"
echo "  git push origin :release/${tag}"
echo 
echo 
echo 
