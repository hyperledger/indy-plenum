#!/usr/bin/env bash

set -x
set -e

BUILD_NUMBER=$1
PKG_SOURCE_PATH=$2
PKG_NAME=indy-plenum

if [[ -z ${BUILD_NUMBER} || -z ${PKG_SOURCE_PATH} ]]; then
    echo "Usage: $0 build-number path-to-package-sources"
    exit 1;
fi

if [ -z $3 ]; then
    CMD=""
else
    CMD=$3
fi


docker build -t indy-plenum-build-u1604 -f Dockerfile .

OUTPUT_VOLUME_NAME=$PKG_NAME-deb-$BUILD_NUMBER
docker volume create --name $OUTPUT_VOLUME_NAME

docker run \
    -i \
    --rm \
    -v ${PKG_SOURCE_PATH}:/input \
    -v ${OUTPUT_VOLUME_NAME}:/output \
    -e PKG_NAME=$PKG_NAME \
    indy-plenum-build-u1604 \
    $CMD

