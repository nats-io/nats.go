FROM golang:1.22
WORKDIR /usr/src/nats.go
COPY . /usr/src/nats.go
RUN go mod tidy -modfile go_test.mod
RUN go test -run TestNone -modfile go_test.mod -tags compat ./test/...
ENV NATS_URL=localhost:4222
ENTRYPOINT ["go", "test", "-v", "-modfile", "go_test.mod", "-tags", "compat", "./test/...", "-count", "1", "-parallel", "10", "-run"]
