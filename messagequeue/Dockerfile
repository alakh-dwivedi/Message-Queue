# --- Stage 1: Build Phase ---
FROM golang: 1.23.2-alpine AS builder

WORKDIR /app

# Cache dependencies
COPY go.mod ./
RUN go mod download

# Copy source tree files
COPY . .

# Build our server target asset binary cleanly
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o queue-server ./cmd/server/

# --- Stage 2: Tiny Executable Runtime Deployment Stage ---
FROM alpine:3.19

WORKDIR /app

# Pull structural output from builder layer
COPY --from=builder /app/queue-server .

# Expose internal TCP system communication port
EXPOSE 9099

# Declare a volume folder path so our WAL data files survive container shutdowns
VOLUME ["/app/data"]

CMD ["./queue-server"]
