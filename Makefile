# Local convenience targets for running tests against the synadia/server-tester
# service. The CI workflow runs the same suite via the `test-tester` job.
#
# Quick start:
#   make tester-up        # start the tester container on a dedicated docker network
#   make test-tester      # run the tagged tests inside a sibling container
#   make tester-down      # tear everything down
#
# To run tests from the host (instead of a sibling container) point your shell at
# the tester's published port and use the testservice build tag:
#   export TESTER_NATS_URL=nats://localhost:4222
#   go test -modfile=go_test.mod -tags=testservice ./...

TESTER_IMAGE   ?= synadia/server-tester:2.14.0
TESTER_NAME    ?= nats-tester
TESTER_NETWORK ?= nats-tester-net
GO_IMAGE       ?= golang:alpine

.PHONY: tester-net tester-up tester-down tester-restart tester-logs test-tester

tester-net:
	@docker network inspect $(TESTER_NETWORK) >/dev/null 2>&1 || \
		docker network create $(TESTER_NETWORK)

tester-up: tester-net
	docker run -d \
		--name $(TESTER_NAME) \
		--network $(TESTER_NETWORK) \
		--restart unless-stopped \
		--sysctl net.ipv4.ip_local_port_range="30000 31000" \
		-p 4222:4222 \
		-p 30000-31000:30000-31000 \
		$(TESTER_IMAGE)
	@echo "Tester running on docker network $(TESTER_NETWORK) as host '$(TESTER_NAME)'"
	@echo "Host-side access: TESTER_NATS_URL=nats://localhost:4222"

# tester-down stops AND removes the container; logs are lost. Use tester-restart
# instead to keep the container (and its logs) around for debugging.
tester-down:
	-docker rm -f $(TESTER_NAME)
	-docker network rm $(TESTER_NETWORK)

tester-restart:
	-docker restart $(TESTER_NAME)
	@echo "Tester restarted; logs preserved (use 'make tester-logs')"

tester-logs:
	docker logs -f $(TESTER_NAME)

test-tester: tester-net
	docker run --rm \
		--network $(TESTER_NETWORK) \
		-v $(CURDIR):/src \
		-w /src \
		-e TESTER_NATS_URL=nats://$(TESTER_NAME):4222 \
		-e CGO_ENABLED=1 \
		$(GO_IMAGE) sh -c '\
			apk add --no-cache gcc libc-dev git make >/dev/null && \
			go test -modfile=go_test.mod -tags=testservice -race -v -p=1 ./test/... ./jetstream/test/... ./micro/test/...'
