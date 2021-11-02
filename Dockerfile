ARG BUILD_EXTENSION=
ARG EXPORT_EXTENSION=latest
FROM rust:1.56.1${BUILD_EXTENSION} AS noria-server

WORKDIR /tmp/noria

COPY . ./

RUN apt-get update && \
    apt-get install -y \
        build-essential \
        libssl-dev \
        linux-libc-dev \
        pkgconf \
        llvm \
        clang \
        default-mysql-client && \
    apt-get clean 

RUN cargo build --release --bin noria-server

ARG EXPORT_EXTENSION=latest
FROM debian:${EXPORT_EXTENSION}

COPY --from=noria-server /tmp/noria/target/release/noria-server /bin/noria-server

ENTRYPOINT /bin/noria-server