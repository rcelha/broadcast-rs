FROM rust:1.76.0-slim-buster as BUILDER

WORKDIR /usr/src/app
ADD . /usr/src/app
RUN --mount=type=cache,target=/usr/src/app/target/ \
    --mount=type=cache,target=/usr/local/cargo/registry/ \
    cargo build --release && \
    cp ./target/release/broadcast-rs /bin/broadcast-rs


FROM debian:12-slim as RUNNER
COPY --from=BUILDER /bin/broadcast-rs /usr/local/bin/broadcast-rs
ENTRYPOINT ["/usr/local/bin/broadcast-rs"]
