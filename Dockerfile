# ---- Builder Stage ----
# Use the official Rust image. The 'slim' variant is smaller and works for most cases.
FROM rust:slim-bookworm AS builder

# Set the working directory.
WORKDIR /usr/src/app

# Install musl tools for static compilation.
RUN apt-get update && apt-get install -y musl-tools
RUN rustup target add x86_64-unknown-linux-musl

# Copy your application's source code.
COPY . .

# Build a statically-linked, release-optimized binary.
RUN cargo build --target x86_64-unknown-linux-musl --release

# ---- Final Stage ----
# Use the 'scratch' image, which is completely empty.
FROM scratch

# Copy the compiled binary from the builder stage
COPY --from=builder /usr/src/app/target/x86_64-unknown-linux-musl/release/server /server

# Set the entrypoint for the container
CMD ["/server"]