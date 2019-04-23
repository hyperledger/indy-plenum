FROM ubuntu:16.04

RUN apt-get update -y && apt-get install -y \
        apt-transport-https \
        ca-certificates

RUN apt-get update -y && apt-get install -y \
    # common stuff
        git \
        wget \
        unzip \
        python3.5 \
        python3-pip \
        python-setuptools \
        python3-venv \
    # fpm
        ruby \
        ruby-dev \
        rubygems \
        gcc \
        make \
    # rocksdb python wrapper
        libbz2-dev \
        zlib1g-dev \
        liblz4-dev \
        libsnappy-dev

RUN pip3 install -U setuptools

# install fpm
RUN gem install --no-ri --no-rdoc rake fpm

WORKDIR /root

ADD . /root
