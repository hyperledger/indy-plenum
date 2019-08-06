FROM hyperledger/indy-core-baseci:0.0.3-master
LABEL maintainer="Hyperledger <hyperledger-indy@lists.hyperledger.org>"

ARG uid=1000
ARG user=indy
ARG venv=venv

RUN echo "To invalidate cache"

RUN apt-get update -y && apt-get install -y \
    python3-nacl \
    cmake \
    autoconf \
    libtool \
    pkg-config \
    libssl-dev \
    libindy=1.10.1~1220 \
# rocksdb python wrapper
    libbz2-dev \
    zlib1g-dev \
    liblz4-dev \
    libsnappy-dev \
    rocksdb=5.8.8


ENV RUST_VERSION=${RUST_VERSION:-1.34.0}

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH

RUN curl https://sh.rustup.rs -sSf | sh -s -- -y --no-modify-path --default-toolchain $RUST_VERSION \
    && chmod -R a+w $RUSTUP_HOME $CARGO_HOME \
    && rustup --version \
    && cargo --version \
    && rustc --version

WORKDIR /home/

RUN curl -fsSL https://github.com/jedisct1/libsodium/releases/download/1.0.14/libsodium-1.0.14.tar.gz | tar -xz

WORKDIR libsodium-1.0.14

RUN ./autogen.sh && ./configure && make && sudo make install

ENV SODIUM_LIB_DIR=/usr/local/lib \
    LD_LIBRARY_PATH=/usr/local/lib

WORKDIR /home/

RUN git clone https://github.com/hyperledger/ursa.git

WORKDIR /home/ursa/

RUN  cargo build --release

RUN cp /home/ursa/target/release/libursa.so /usr/lib/.

RUN indy_ci_add_user $uid $user $venv

RUN indy_image_clean

USER $user
WORKDIR /home/$user
