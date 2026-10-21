FROM golang:1.27-alpine AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/workflow-api ./cmd/workflow-api
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/workflow-demo ./cmd/workflow-demo

FROM alpine:3.23 AS runtime
RUN apk add --no-cache ca-certificates
WORKDIR /app
USER 65532:65532

FROM runtime AS workflow-api
COPY --from=build --chown=65532:65532 /out/workflow-api /app/workflow-api
EXPOSE 5080
HEALTHCHECK --interval=2s --timeout=2s --start-period=5s --retries=15 \
  CMD wget -q -O /dev/null http://127.0.0.1:5080/health/ready || exit 1
ENTRYPOINT ["/app/workflow-api"]

FROM runtime AS workflow-demo
COPY --from=build --chown=65532:65532 /out/workflow-demo /app/workflow-demo
EXPOSE 5090
HEALTHCHECK --interval=2s --timeout=2s --start-period=5s --retries=15 \
  CMD wget -q -O /dev/null http://127.0.0.1:5090/health/ready || exit 1
ENTRYPOINT ["/app/workflow-demo"]
