FROM golang:1.27-alpine AS build
WORKDIR /src

COPY go.mod ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/workflow-api ./cmd/workflow-api

FROM gcr.io/distroless/static-debian13:nonroot
COPY --from=build /out/workflow-api /workflow-api
EXPOSE 5080
ENTRYPOINT ["/workflow-api"]
