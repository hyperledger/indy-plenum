#!/usr/bin/env bash

set -e
set -x

OUTPUT_PATH=${1:-.}
wheel2debconf="$(dirname "$(realpath "$0")")"/wheel2deb.yml


function build_rocksdb_deb {
    VERSION=$1
    VERSION_TAG="rocksdb-$VERSION"
    #Install rocksdb requirements (libsnappy lbz2 llz4)
    apt update && apt install -y libsnappy-dev libbz2-dev liblz4-dev zlib1g-dev libgflags-dev
    git clone https://github.com/evernym/rocksdb.git /tmp/rocksdb
    scriptpath="$(dirname "$(realpath "$0")")"/make_rocksdb.sh
    cd /tmp/rocksdb
    git checkout $VERSION_TAG
    cp $scriptpath /tmp/rocksdb/build_tools/make_package.sh
    sed -i 's/-m rocksdb@fb.com/-m "Hyperledger <hyperledger-indy@lists.hyperledger.org>"/g' \
        ./build_tools/make_package.sh
    PORTABLE=1 EXTRA_CFLAGS="-fPIC" EXTRA_CXXFLAGS="-fPIC" ./build_tools/make_package.sh $VERSION
    # Install it in the system as it is needed by python-rocksdb.
    make install
    cd -
    cp /tmp/rocksdb/package/rocksdb_${VERSION}_amd64.deb $OUTPUT_PATH
    rm -rf /tmp/rocksdb
}

function build_ioflo_deb {
    VERSION=$1

    git clone https://github.com/reflectivedevelopment/ioflo.git /tmp/ioflo
    pushd /tmp/ioflo
    git checkout $VERSION

    python3 setup.py bdist_wheel
    pushd dist
    wheel2deb --config ${wheel2debconf}
    mkdir -p ${OUTPUT_PATH}
    mv output/*.deb ${OUTPUT_PATH}

    popd
    rm -rf /tmp/ioflo
    popd
}

function build_from_pypi_fpm {
    PACKAGE_NAME=$1

    if [ -z $2 ]; then
        PACKAGE_VERSION=""
        # Get the most recent package version from PyPI to be included in the package name of the Debian artifact
        curl -X GET "https://pypi.org/pypi/${PACKAGE_NAME}/json" > "${PACKAGE_NAME}.json"
        PACKAGE_VERSION="==$(cat "${PACKAGE_NAME}.json" | jq --raw-output '.info.version')"
        rm "${PACKAGE_NAME}.json"
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
            --python-setup-py-arguments "${3}" \
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

            # --python-pip "$(which pip)" \
        # ERROR:  download_if_necessary': Unexpected directory layout after easy_install. Maybe file a bug? The directory is /tmp/package-python-build-c42d23109dcca1e98d9f430a04fe79a815f10d8ed7a719633aa969424f94 (RuntimeError)
    fi

    rm ${POSTINST_TMP}
    rm ${PREREM_TMP}
}

function build_from_pypi_wheel {
    PACKAGE_NAME=$1

    if [ -z $2 ]; then
        PACKAGE_VERSION=""
        # Get the most recent package version from PyPI to be included in the package name of the Debian artifact
        curl -X GET "https://pypi.org/pypi/${PACKAGE_NAME}/json" > "${PACKAGE_NAME}.json"
        PACKAGE_VERSION="==$(cat "${PACKAGE_NAME}.json" | jq --raw-output '.info.version')"
        rm "${PACKAGE_NAME}.json"
    else
        PACKAGE_VERSION="==$2"
    fi

    rm -rvf /tmp/wheel
    mkdir /tmp/wheel
    pushd /tmp/wheel
    pip3 wheel ${PACKAGE_NAME}${PACKAGE_VERSION}
    # Can't build cytoolz using wheel for rlp, but can't build rlp with fpm
    rm -f /tmp/wheel/cytoolz*
    wheel2deb --config ${wheel2debconf}
    mkdir -p ${OUTPUT_PATH}
    popd
    mv /tmp/wheel/output/*.deb ${OUTPUT_PATH}
    rm -rvf /tmp/wheel
}


# TODO duplicates list from Jenkinsfile.cd
SCRIPT_PATH="${BASH_SOURCE[0]}"
pushd `dirname ${SCRIPT_PATH}` >/dev/null

# Install any python requirements needed for the builds.
pip install -r requirements.txt
pip3 install wheel2deb && apt-get install -y debhelper
apt-get install -y cython3

# Build rocksdb at first
### Can be removed once the code has been updated to run with rocksdb v. 5.17
### Issue 1551: Update RocksDB; https://github.com/hyperledger/indy-plenum/issues/1551
build_rocksdb_deb 5.8.8

#### PyZMQCommand
build_from_pypi_fpm pyzmq 22.3.0 --zmq=bundled

##### install_requires
build_from_pypi_wheel base58
### Needs to be pinned to 3.10.1 because from v4.0.0 the package name ends in python3-importlib-metadata_0.0.0_amd64.deb
### https://github.com/hyperledger/indy-plenum/runs/4166593170?check_suite_focus=true#step:5:5304
build_from_pypi_wheel importlib-metadata 3.10.1
build_ioflo_deb 2.0.3
build_from_pypi_wheel jsonpickle
build_from_pypi_wheel leveldb
build_from_pypi_wheel libnacl 1.6.1
build_from_pypi_wheel msgpack-python
build_from_pypi_wheel orderedset
build_from_pypi_wheel packaging 21.3
build_from_pypi_wheel portalocker 2.7.0
build_from_pypi_wheel prompt-toolkit 3.0.18
build_from_pypi_fpm psutil
build_from_pypi_wheel pympler 0.8
build_from_pypi_wheel python-dateutil
build_from_pypi_wheel python-rocksdb
build_from_pypi_wheel python-ursa 0.1.1
build_from_pypi_wheel rlp 2.0.0
build_from_pypi_fpm cytoolz 0.12.3
build_from_pypi_wheel semver 2.13.0
build_from_pypi_wheel sha3
build_from_pypi_wheel six
build_from_pypi_wheel sortedcontainers 2.1.0
build_from_pypi_wheel ujson 1.33

rm -vf ${OUTPUT_PATH}/python3-setuptools*.deb