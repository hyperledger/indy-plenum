#!/bin/bash -xe

INPUT_PATH=$1
VERSION=$2
OUTPUT_PATH=${3:-.}
PACKAGE_VERSION=${4:-$VERSION}

PACKAGE_NAME=indy-plenum

# copy the sources to a temporary folder
TMP_DIR=$(mktemp -d)
cp -r ${INPUT_PATH}/. ${TMP_DIR}

# prepare the sources
cd ${TMP_DIR}/build-scripts/ubuntu-1604
./prepare-package.sh ${TMP_DIR} ${VERSION}

sed -i 's/{package_name}/'${PACKAGE_NAME}'/' "postinst"
sed -i 's/{package_name}/'${PACKAGE_NAME}'/' "prerm"

fpm --input-type "python" \
    --output-type "deb" \
    --architecture "amd64" \
    --verbose \
    --python-package-name-prefix "python3"\
    --python-bin "/usr/bin/python3" \
    --exclude "usr/local/lib/python3.5/dist-packages/data" \
    --exclude "usr/local/bin" \
    --exclude "*.pyc" \
    --exclude "*.pyo" \
    --maintainer "Hyperledger <hyperledger-indy@lists.hyperledger.org>" \
    --after-install "postinst" \
    --before-remove "prerm" \
    --name ${PACKAGE_NAME} \
    --version ${PACKAGE_VERSION} \
    --package ${OUTPUT_PATH} \
    ${TMP_DIR}

rm -rf ${TMP_DIR}
