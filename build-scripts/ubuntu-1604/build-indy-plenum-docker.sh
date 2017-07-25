#!/usr/bin/env bash

set -x
set -e

PKG_SOURCE_PATH=$1
PKG_NAME=indy-plenum

if [ -z ${PKG_SOURCE_PATH} ]; then
    echo "Usage: $0 path-to-package-sources"
    exit 1;
fi

if [ -z $2 ]; then
    CMD="/root/build-indy-plenum.sh /input /output"
else
    CMD=$2
fi

docker build -t indy-plenum-build-u1604 -f Dockerfile .

OUTPUT_VOLUME_NAME=${PKG_NAME}-deb-u1604
docker volume create --name ${OUTPUT_VOLUME_NAME}

docker run \
    -i \
    --rm \
    -v ${PKG_SOURCE_PATH}:/input \
    -v ${OUTPUT_VOLUME_NAME}:/output \
    -e PKG_NAME=${PKG_NAME} \
    indy-plenum-build-u1604 \
    $CMD

