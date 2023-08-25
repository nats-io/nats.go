FROM golang:1.20
WORKDIR /usr/src/nats.go
COPY . /usr/src/nats.go
RUN go mod tidy -modfile go_test.mod
RUN go test -run TestNone -modfile go_test.mod -tags compat ./test/...
ENV NATS_URL=localhost:4222
CMD go test -v -run TestCompatibility -modfile go_test.mod -tags compat ./test/... -count 1 -parallel 10
