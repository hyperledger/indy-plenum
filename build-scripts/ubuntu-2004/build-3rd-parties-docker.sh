#!/usr/bin/env bash

set -x
set -e


PKG_SOURCE_PATH=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )/../../

if [ -z "$2" ]; then
    CMD="/input/build-scripts/ubuntu-2004/build-3rd-parties.sh /output"
else
    CMD="$2"
fi

PKG_NAME=indy-plenum
IMAGE_NAME="${PKG_NAME}-build-u2004"
OUTPUT_VOLUME_NAME="${1:-"${PKG_NAME}-deb-u2004"}"

docker build -t "${PKG_NAME}-build-u2004" -f $PKG_SOURCE_PATH/.github/workflows/build/Dockerfile.ubuntu-2004 .
docker volume create --name "${OUTPUT_VOLUME_NAME}"

docker run \
    -i \
    --rm \
    -v "${PKG_SOURCE_PATH}:/input" \
    -v "${OUTPUT_VOLUME_NAME}:/output" \
    "${IMAGE_NAME}" \
    $CMD
