FROM hyperledger/indy-core-baseci:0.0.3-master
LABEL maintainer="Hyperledger <hyperledger-indy@lists.hyperledger.org>"

ARG uid=1000
ARG user=indy
ARG venv=venv

RUN echo "To invalidate cache"

RUN wget http://archive.ubuntu.com/ubuntu/pool/main/o/openssl1.0/libssl1.0.0_1.0.2n-1ubuntu5.3_amd64.deb  && \
    dpkg -i libssl1.0.0_1.0.2n-1ubuntu5.3_amd64.deb 

WORKDIR /root

RUN cd /usr/lib/x86_64-linux-gnu \
    && ln -s libssl.so.1.0.0 libssl.so.10 \
    && ln -s libcrypto.so.1.0.0 libcrypto.so.10 \
    && curl -fsSL https://github.com/jedisct1/libsodium/archive/1.0.18.tar.gz | tar -xz \
    && cd libsodium-1.0.18 \
    && ./autogen.sh \
    && ./configure \
    && make install \
    && cd .. \  
    && rm -rf libsodium-1.0.18 

RUN apt-get update -y && apt-get install -y \
    python3-nacl \
    libindy=1.13.0~1420 \
    libssl-dev \
# rocksdb python wrapper
    libbz2-dev \
    zlib1g-dev \
    liblz4-dev \
    libsnappy-dev \
    ursa=0.3.2-2  \
    rocksdb=5.8.8

RUN indy_ci_add_user $uid $user $venv

RUN indy_image_clean

USER $user
WORKDIR /home/$user

