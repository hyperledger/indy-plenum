FROM hyperledger/indy-core-baseci:0.0.3-master
LABEL maintainer="Hyperledger <hyperledger-indy@lists.hyperledger.org>"

ARG uid=1000
ARG user=indy
ARG venv=venv

RUN echo "To invalidate cache"


 RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys CE7709D068DB5E88
 RUN add-apt-repository "deb https://repo.sovrin.org/ursa/deb xenial master"

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
    ursa \
    rocksdb=5.8.8



RUN curl -fsSL https://github.com/jedisct1/libsodium/releases/download/1.0.14/libsodium-1.0.14.tar.gz | tar -xz

WORKDIR libsodium-1.0.14

RUN ./autogen.sh && ./configure && make && sudo make install

ENV SODIUM_LIB_DIR=/usr/local/lib \
    LD_LIBRARY_PATH=/usr/local/lib

WORKDIR /home/

