FROM hyperledger/indy-core-baseci:0.0.3-master
LABEL maintainer="Hyperledger <hyperledger-indy@lists.hyperledger.org>"

ARG uid=1000
ARG user=indy
ARG venv=venv

RUN echo "To invalidate cache"

RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys CE7709D068DB5E88
RUN echo "deb https://repo.sovrin.org/ursa/deb xenial master" >> /etc/apt/sources.list && apt-get update


RUN apt-get update -y && apt-get install -y \
    python3-nacl \
    cmake \
    autoconf \
    libtool \
    pkg-config \
    libssl-dev \
    libindy=1.11.1~1343 \
# rocksdb python wrapper
    libbz2-dev \
    zlib1g-dev \
    liblz4-dev \
    libsnappy-dev \
    ursa  \
    rocksdb=5.8.8


WORKDIR /home/

