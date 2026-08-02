FROM rust:latest AS builder
WORKDIR /app
COPY . .

# Use BuildKit cache mounts to instantly cache the Cargo registry and the target directory.
# This ensures that even if source code changes, dependencies are NEVER re-downloaded or re-compiled!
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build -p server && \
    cp target/debug/server /usr/local/bin/vein-server

# Stage 2: Lightweight runtime image
FROM rust:slim
# Install openssl for cert generation and QUIC requirements (libssl)
RUN apt-get update && apt-get install -y openssl ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
# Copy the built binary
COPY --from=builder /usr/local/bin/vein-server /usr/local/bin/vein-server

CMD ["vein-server"]
