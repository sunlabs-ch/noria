ARG EXTENSION=
FROM rust:1.56.1${EXTENSION} AS noria-server

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

RUN cargo build --release --workspace