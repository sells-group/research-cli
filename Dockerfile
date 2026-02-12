FROM golang:1.23-alpine AS builder

RUN apk add --no-cache ca-certificates

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -tags=integration -o research-cli ./cmd

FROM alpine:3.19

RUN apk add --no-cache ca-certificates tzdata

COPY --from=builder /build/research-cli /usr/local/bin/research-cli

ENTRYPOINT ["research-cli"]
CMD ["serve"]
