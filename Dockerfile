FROM rust:1.76.0-slim-buster as BUILDER

WORKDIR /usr/src/app
ADD . /usr/src/app
RUN cargo build --release

FROM debian:12-slim as RUNNER
COPY --from=BUILDER /usr/src/app/target/release/broadcast-rs /usr/local/bin/broadcast-rs
ENTRYPOINT ["/usr/local/bin/broadcast-rs"]
