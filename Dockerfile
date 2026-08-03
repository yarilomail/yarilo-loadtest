FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
ARG BUILD_VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-s -w -X main.version=${BUILD_VERSION}" -trimpath \
    -o /out/yarilo-loadtest .

FROM alpine:3.23
RUN adduser -D -u 1000 loadtest
COPY --from=builder /out/yarilo-loadtest /usr/local/bin/yarilo-loadtest
USER 1000
ENTRYPOINT ["/usr/local/bin/yarilo-loadtest"]
