#!/usr/bin/env bash

set -x
set -e

INPUT_PATH=$1
OUTPUT_PATH=${2:-.}

PACKAGE_NAME=indy-plenum
POSTINST_TMP=postinst-${PACKAGE_NAME}
PREREM_TMP=prerm-${PACKAGE_NAME}

cp postinst ${POSTINST_TMP}
cp prerm ${PREREM_TMP}
sed -i 's/{package_name}/'${PACKAGE_NAME}'/' ${POSTINST_TMP}
sed -i 's/{package_name}/'${PACKAGE_NAME}'/' ${PREREM_TMP}

fpm --input-type "python" \
    --output-type "deb" \
    --architecture "amd64" \
    --verbose \
    --maintainer "Evernym <dev@evernym.com>" \
    --python-package-name-prefix "python3"\
    --python-bin "/usr/bin/python3" \
    --exclude "usr/local/lib/python3.5/dist-packages/data" \
    --exclude "usr/local/bin" \
    --exclude "*.pyc" \
    --exclude "*.pyo" \
    --after-install ${POSTINST_TMP} \
    --before-remove ${PREREM_TMP} \
    --name ${PACKAGE_NAME} \
    --package ${OUTPUT_PATH} \
    ${INPUT_PATH}

rm ${POSTINST_TMP}
rm ${PREREM_TMP}
