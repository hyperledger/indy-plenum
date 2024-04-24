# See here for image contents: https://github.com/microsoft/vscode-dev-containers/tree/v0.209.6/containers/ubuntu/.devcontainer/base.Dockerfile

# [Choice] Ubuntu version (use hirsuite or bionic on local arm64/Apple Silicon): hirsute, focal, bionic
ARG VARIANT="jammy"
FROM mcr.microsoft.com/vscode/devcontainers/base:${VARIANT}

# [Optional] Uncomment this section to install additional OS packages.
# RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
#     && apt-get -y install --no-install-recommends <your-package-list-here>

RUN apt-get update -y && apt-get install -y \
    # common stuff
    git \
    wget \
    gnupg \
    apt-transport-https \
    ca-certificates \
    apt-utils \
    curl \
    jq

# ========================================================================================================
# Update repository signing keys
# --------------------------------------------------------------------------------------------------------
    # Hyperledger
RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 9692C00E657DDE61 && \
    # Sovrin
    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys CE7709D068DB5E88 && \
    # bionic-security
    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 3B4FE6ACC0B21F32
# ========================================================================================================

# Plenum
#  - https://github.com/hyperledger/indy-plenum/issues/1546
#  - Needed to pick up rocksdb=5.8.8
RUN echo "deb  https://hyperledger.jfrog.io/artifactory/indy focal dev"  >> /etc/apt/sources.list && \
    echo "deb http://security.ubuntu.com/ubuntu bionic-security main"  >> /etc/apt/sources.list && \
    echo "deb https://repo.sovrin.org/deb bionic master" >> /etc/apt/sources.list && \
    echo "deb https://repo.sovrin.org/sdk/deb bionic master" >> /etc/apt/sources.list

# Kim's temp repo
# TODO:
#    - Remove dependency on Kim's temp repo.
#    - Change this to official repo
RUN echo "deb [trusted=yes] http://209.141.41.82:8000/ packagedir/" >> /etc/apt/sources.list

RUN apt-get update -y && apt-get install -y \
    # Python
    python3-pip \
    python3-nacl \
    # rocksdb python wrapper
    rocksdb=5.8.8 \
    libgflags-dev \
    libsnappy-dev \
    zlib1g-dev \
    libbz2-dev \
    liblz4-dev \
    libgflags-dev \
    # zstd is needed for caching in github actions pipeline
    zstd \
    # fpm
    ruby \
    ruby-dev \
    rubygems \
    gcc \
    make \
    # Indy Node and Plenum
    libssl1.0.0 \
    ursa=0.3.2-1 \
    # Indy SDK
    libindy=1.15.0~1625-bionic \
    # Kim's updated packages
    #  - TODO: Remove dependency on Kim's temp repo.
    python3-sortedcontainers \
    python3-rlp \
    python3-ioflo \
    # Need to move libursa.so to parent dir
    && mv /usr/lib/ursa/* /usr/lib && rm -rf /usr/lib/ursa

RUN pip3 install -U \
    # Required by setup.py
    setuptools==50.3.2 \
    'pyzmq==22.3.0'\
    # Linting tools
    pep8==1.7.1 \
    pep8-naming==0.6.1 \
    flake8==3.8.4 \
    Cython==0.29.36


# install fpm
RUN gem install --no-document rake dotenv:2.8.1 fpm:1.15.0
