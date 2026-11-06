FROM golang:1.27-alpine AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/dispatch-api ./cmd/dispatch-api
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/dispatch-demo ./cmd/dispatch-demo

FROM alpine:3.23 AS runtime
RUN apk add --no-cache ca-certificates
WORKDIR /app
USER 65532:65532

FROM runtime AS dispatch-api
COPY --from=build --chown=65532:65532 /out/dispatch-api /app/dispatch-api
EXPOSE 5080
HEALTHCHECK --interval=2s --timeout=2s --start-period=5s --retries=15 \
  CMD wget -q -O /dev/null http://127.0.0.1:5080/health/ready || exit 1
ENTRYPOINT ["/app/dispatch-api"]

FROM runtime AS dispatch-demo
COPY --from=build --chown=65532:65532 /out/dispatch-demo /app/dispatch-demo
EXPOSE 5090
HEALTHCHECK --interval=2s --timeout=2s --start-period=5s --retries=15 \
  CMD wget -q -O /dev/null http://127.0.0.1:5090/health/ready || exit 1
ENTRYPOINT ["/app/dispatch-demo"]
