FROM golang:1.23-alpine AS builder

RUN apk add --no-cache ca-certificates

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -tags=integration -o research-cli ./cmd

# Download Atlas binary for schema management.
ARG ATLAS_VERSION=latest
RUN wget -qO /usr/local/bin/atlas \
    "https://release.ariga.io/atlas/atlas-linux-amd64-${ATLAS_VERSION}" && \
    chmod +x /usr/local/bin/atlas

FROM alpine:3.19

RUN apk add --no-cache ca-certificates tzdata

COPY --from=builder /build/research-cli /usr/local/bin/research-cli
COPY --from=builder /usr/local/bin/atlas /usr/local/bin/atlas

ENTRYPOINT ["research-cli"]
CMD ["serve"]
