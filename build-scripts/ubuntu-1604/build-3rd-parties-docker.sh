#!/usr/bin/env bash

set -x
set -e

if [ -z $1 ]; then
    CMD="/root/build-3rd-parties.sh /output"
else
    CMD=$1
fi

PKG_NAME=indy-plenum

docker build -t indy-plenum-build-u1604 -f Dockerfile .

OUTPUT_VOLUME_NAME=${PKG_NAME}-deb-u1604
docker volume create --name ${OUTPUT_VOLUME_NAME}

docker run \
    -i \
    --rm \
    -v ${OUTPUT_VOLUME_NAME}:/output \
    indy-plenum-build-u1604 \
    $CMD
