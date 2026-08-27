## Pinned to the patch go.mod names: a scan reads go.mod, the image ships the
## base, and a floating base makes them disagree silently (yarilo#1497).
## Dependabot moves it.
FROM golang:1.26.7-alpine AS builder

## A newer go.mod must fail the build, not fetch a toolchain.
ENV GOTOOLCHAIN=local
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
ARG BUILD_VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-s -w -X main.version=${BUILD_VERSION}" -trimpath \
    -o /out/yarilo-loadtest .

FROM alpine:3.24
RUN adduser -D -u 1000 loadtest
COPY --from=builder /out/yarilo-loadtest /usr/local/bin/yarilo-loadtest
USER 1000
ENTRYPOINT ["/usr/local/bin/yarilo-loadtest"]
