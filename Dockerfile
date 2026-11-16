## Pinned to the patch go.mod names: a scan reads go.mod, the image ships the
## base, and a floating base makes them disagree silently (yarilo#1497).
## Dependabot moves it.
FROM golang:1.27.4-alpine3.24@sha256:0a5f7fe796599ff487605987432e7619ae45beeb2fba4c2ce0b907879932aebf AS builder

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

# Named so CI can rebuild it without its layer cache: a cached apk upgrade
# keeps the packages of its first build until the base digest moves.
FROM alpine:3.24@sha256:f59abe656b692d901e184be3445e084786602108599d2a51d229c0f5a48c71bf AS runtime
# Upgrade before adding anything: a fixed package reaches every branch as a
# patch, while the image under a minor tag lags behind it. Moving 3.23 -> 3.24
# did not close CVE-2026-14456 on its own -- pulling current packages at build
# time does, and without this the scan is red again the moment the tag is
# rebuilt. Same reason as the product image.
#
# hadolint ignore=DL3018,DL3019
RUN apk upgrade --no-cache && adduser -D -u 1000 loadtest
COPY --from=builder /out/yarilo-loadtest /usr/local/bin/yarilo-loadtest
USER 1000
ENTRYPOINT ["/usr/local/bin/yarilo-loadtest"]
