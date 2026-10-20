FROM golang:1.24-alpine AS builder

WORKDIR /app

RUN apk add --no-cache git gcc musl-dev

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /app/server ./cmd/server
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /app/loadtest ./cmd/loadtest

FROM alpine:3.20

RUN apk add --no-cache ca-certificates curl jq

WORKDIR /app

COPY --from=builder /app/server /app/server
COPY --from=builder /app/loadtest /app/loadtest
COPY --from=builder /app/migrations /app/migrations
COPY --from=builder /app/docs /app/docs

EXPOSE 8080

ENTRYPOINT ["/app/server"]
