FROM golang:1.25 AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/coordinator ./cmd/coordinator && \
    CGO_ENABLED=0 GOOS=linux go build -o /out/broker      ./cmd/broker      && \
    CGO_ENABLED=0 GOOS=linux go build -o /out/throughput  ./cmd/throughput

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates dnsutils && rm -rf /var/lib/apt/lists/*
COPY --from=builder /out/ /usr/local/bin/
