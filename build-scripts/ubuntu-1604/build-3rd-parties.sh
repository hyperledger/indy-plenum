#!/usr/bin/env bash

set -e
set -x

OUTPUT_PATH=${1:-.}

function build_rocksdb_deb {
    VERSION=$1
    VERSION_TAG="rocksdb-$VERSION"

    git clone https://github.com/evernym/rocksdb.git /tmp/rocksdb
    cd /tmp/rocksdb
    git checkout $VERSION_TAG
    sed -i 's/-m rocksdb@fb.com/-m "Hyperledger <hyperledger-indy@lists.hyperledger.org>"/g' \
        ./build_tools/make_package.sh
    PORTABLE=1 EXTRA_CFLAGS="-fPIC" EXTRA_CXXFLAGS="-fPIC" ./build_tools/make_package.sh $VERSION
    cp ./package/rocksdb_${VERSION}_amd64.deb $OUTPUT_PATH
    # Install it in the system as it is needed by python-rocksdb.
    make install
    cd -
    rm -rf /tmp/rocksdb
}

function build_from_pypi {
    PACKAGE_NAME=$1

    if [ -z $2 ]; then
        PACKAGE_VERSION=""
    else
        PACKAGE_VERSION="==$2"
    fi
    POSTINST_TMP=postinst-${PACKAGE_NAME}
    PREREM_TMP=prerm-${PACKAGE_NAME}
    cp postinst ${POSTINST_TMP}
    cp prerm ${PREREM_TMP}
    if [[ ${PACKAGE_NAME} =~ ^python-* ]]; then
        PACKAGE_NAME_TMP="${PACKAGE_NAME/python-/}"
    else
        PACKAGE_NAME_TMP=$PACKAGE_NAME
    fi
    sed -i 's/{package_name}/python3-'${PACKAGE_NAME_TMP}'/' ${POSTINST_TMP}
    sed -i 's/{package_name}/python3-'${PACKAGE_NAME_TMP}'/' ${PREREM_TMP}

    if [ -z $3 ]; then
        fpm --input-type "python" \
            --output-type "deb" \
            --architecture "amd64" \
            --verbose \
            --python-package-name-prefix "python3"\
            --python-bin "/usr/bin/python3" \
            --exclude "*.pyc" \
            --exclude "*.pyo" \
            --maintainer "Hyperledger <hyperledger-indy@lists.hyperledger.org>" \
            --after-install ${POSTINST_TMP} \
            --before-remove ${PREREM_TMP} \
            --package ${OUTPUT_PATH} \
            ${PACKAGE_NAME}${PACKAGE_VERSION}
    else
        fpm --input-type "python" \
            --output-type "deb" \
            --architecture "amd64" \
            --python-setup-py-arguments "--zmq=bundled" \
            --verbose \
            --python-package-name-prefix "python3"\
            --python-bin "/usr/bin/python3" \
            --exclude "*.pyc" \
            --exclude "*.pyo" \
            --maintainer "Hyperledger <hyperledger-indy@lists.hyperledger.org>" \
            --after-install ${POSTINST_TMP} \
            --before-remove ${PREREM_TMP} \
            --package ${OUTPUT_PATH} \
            ${PACKAGE_NAME}${PACKAGE_VERSION}
    fi

    rm ${POSTINST_TMP}
    rm ${PREREM_TMP}
}

# TODO duplicates list from Jenkinsfile.cd

# Build rocksdb at first
build_rocksdb_deb 5.8.8

build_from_pypi ioflo 1.5.4
build_from_pypi orderedset 2.0.3
build_from_pypi base58 1.0.0
build_from_pypi prompt-toolkit 0.57
build_from_pypi rlp 0.5.1
build_from_pypi sha3 0.2.1
build_from_pypi libnacl 1.6.1
build_from_pypi six 1.11.0
build_from_pypi portalocker 0.5.7
build_from_pypi sortedcontainers 1.5.7
build_from_pypi setuptools 38.5.2
build_from_pypi python-dateutil 2.6.1
build_from_pypi semver 2.7.9
build_from_pypi pygments 2.2.0
build_from_pypi psutil 5.6.6
build_from_pypi pyzmq 18.1.0 bundled
build_from_pypi intervaltree 2.1.0
build_from_pypi jsonpickle 0.9.6
# TODO: add libsnappy dependency for python-rocksdb package
build_from_pypi python-rocksdb 0.6.9
build_from_pypi pympler 0.8
build_from_pypi packaging 19.0
build_from_pypi python-ursa 0.1.1
