FROM --platform=$BUILDPLATFORM rust:bookworm AS builder
WORKDIR /app

ARG BUILD_TARGET=dev

RUN apt-get update && apt-get install -y clang

COPY . .

RUN cargo build --$BUILD_TARGET --bin server

FROM --platform=$BUILDPLATFORM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y ca-certificates
COPY --from=builder /app/target/release/server /server
CMD ["/server"]
