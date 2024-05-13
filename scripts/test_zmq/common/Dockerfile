FROM ubuntu:24.04

RUN apt update && apt install -y \
     python3 \
     python3-pip \
     python3-setuptools \
     python3-nacl

# pypi based packages
RUN pip3 install -U \
    pip \
    setuptools \
    virtualenv