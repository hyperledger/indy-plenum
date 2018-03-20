FROM ubuntu:16.04

RUN apt-get update -y && apt-get install -y \
        apt-transport-https \
        ca-certificates

RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 68DB5E88 && \
    echo "deb https://repo.sovrin.org/test/deb xenial rocksdb" >> /etc/apt/sources.list && \
    apt-get update

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
        libsnappy-dev \
        rocksdb=5.8.8

RUN pip3 install -U setuptools

# install fpm
RUN gem install --no-ri --no-rdoc fpm

WORKDIR /root

ADD . /root
