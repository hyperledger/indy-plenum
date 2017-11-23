#!/bin/bash
set -e
set -x

# TODO should be deprecated once base images are pushed to dockerhub

# should be called from the root of the repo

BASE_IMAGE=hyperledger/indy-core-baseci
INDY_NODE_DIR=indy_node

INDY_NODE_REPO_URL=https://github.com/hyperledger/indy-node.git
INDY_NODE_REPO_BRANCH=master


build_from_dockerfiles() {
    mkdir $INDY_NODE_DIR

    pushd $INDY_NODE_DIR
    git init
    git remote add --no-tags origin $INDY_NODE_REPO_URL
    git config core.sparseCheckout true
    echo "docker-files/baseimage" >> .git/info/sparse-checkout
    git fetch --no-tags origin $INDY_NODE_REPO_BRANCH
    git checkout $INDY_NODE_REPO_BRANCH
    make -C docker-files/baseimage clean all
    popd

    rm -rf $INDY_NODE_DIR
}

docker pull "$BASE_IMAGE" || build_from_dockerfiles
