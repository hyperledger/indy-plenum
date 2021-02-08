#!/bin/bash -xe

apt-get install -y software-properties-common \
    && apt-get update \
    && add-apt-repository -y ppa:git-core/ppa \
    && apt-get update \
    && apt-get install -y git
