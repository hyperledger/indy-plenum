#!/bin/bash -xe

apt-get update -y && apt-get install -y \
        apt-transport-https \
        ca-certificates

apt-get update -y && apt-get install -y \
        git \
        wget \
        unzip \
        python3.5 \
        python3-pip \
        python3-venv \
        ruby \
        ruby-dev \
        rubygems \
        gcc \
        make \
        libbz2-dev \
        zlib1g-dev \
        liblz4-dev \
        libsnappy-dev \
    && rm -rf /var/lib/apt/lists/*
