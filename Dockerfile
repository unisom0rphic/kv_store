FROM rust:1.93.0-slim AS builder
WORKDIR /usr/src/app

# Apparently this is some clever trick with caching idk I just trust it
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release

COPY ./src ./src
RUN cargo build --release

# Runtime
FROM debian:stable-slim
WORKDIR /usr/src/app
COPY --from=builder /usr/src/app/target/release/kv_store .
EXPOSE 6767
CMD ["./kv_store"]