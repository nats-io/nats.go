# Local convenience targets for the testservice PoC.
# Documented in docs/superpowers/plans/2026-05-21-testservice-poc.md.

TESTER_IMAGE   ?= synadia/server-tester:2.14.0
TESTER_NAME    ?= nats-tester
TESTER_NETWORK ?= nats-tester-net
GO_IMAGE       ?= golang:alpine

.PHONY: tester-net tester-up tester-down tester-logs test-tester

tester-net:
	@docker network inspect $(TESTER_NETWORK) >/dev/null 2>&1 || \
		docker network create $(TESTER_NETWORK)

tester-up: tester-net
	docker run -d --rm \
		--name $(TESTER_NAME) \
		--network $(TESTER_NETWORK) \
		--sysctl net.ipv4.ip_local_port_range="30000 31000" \
		-p 4222:4222 \
		-p 30000-31000:30000-31000 \
		$(TESTER_IMAGE)
	@echo "Tester running on docker network $(TESTER_NETWORK) as host '$(TESTER_NAME)'"
	@echo "Host-side access: TESTER_NATS_URL=nats://localhost:4222"

tester-down:
	-docker rm -f $(TESTER_NAME)
	-docker network rm $(TESTER_NETWORK)

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
			go test -modfile=go_test.mod -tags=testservice -race -v -p=1 ./jetstream/test/... -run TestTester'
