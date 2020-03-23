FROM hyperledger/indy-core-baseci:0.0.3-master
LABEL maintainer="Hyperledger <hyperledger-indy@lists.hyperledger.org>"

ARG uid=1000
ARG user=indy
ARG venv=venv

RUN echo "To invalidate cache"


RUN apt-get update -y && apt-get install -y \
    python3-nacl \
    libindy=1.13.0~1420 \
    libssl-dev \
# rocksdb python wrapper
    libbz2-dev \
    zlib1g-dev \
    liblz4-dev \
    libsnappy-dev \
    ursa=0.3.2-1  \
    rocksdb=5.8.8

RUN indy_ci_add_user $uid $user $venv

RUN indy_image_clean

USER $user
WORKDIR /home/

