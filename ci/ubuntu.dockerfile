FROM hyperledger/indy-core-baseci:0.0.1
LABEL maintainer="Hyperledger <hyperledger-indy@lists.hyperledger.org>"

ARG uid=1000
ARG user=indy
ARG venv=venv

RUN apt-get update -y && apt-get install -y \
    python3-nacl \
    libindy-crypto=0.2.0 \
    libindy=1.3.1~403

RUN indy_ci_add_user $uid $user $venv

####### FIXME: this code of rocksdb and python-rocksdb installation should be re-factored #######
RUN apt-get update -y && apt-get install -y \
    autoconf \
    build-essential \
    libtool-bin \
    zlib1g-dev \
    libbz2-dev \
    pkg-config \
    python3-dev

RUN pip install -U Cython

USER $user
WORKDIR /home/indy
RUN git clone https://github.com/evernym/snappy.git
WORKDIR /home/indy/snappy
RUN ./autogen.sh && ./configure && cat ./README.md > ./README
RUN make

USER root
RUN make install

USER $user
WORKDIR /home/indy
RUN git clone https://github.com/evernym/rocksdb.git
WORKDIR /home/indy/rocksdb
RUN make EXTRA_CFLAGS="-fPIC" EXTRA_CXXFLAGS="-fPIC" static_lib

USER root
RUN make install

USER $user
WORKDIR /home/indy
RUN git clone https://github.com/evernym/python-rocksdb.git
WORKDIR /home/indy/python-rocksdb
RUN python setup.py build
USER root
RUN python setup.py install
RUN ldconfig

####### FIXME end #######

RUN indy_image_clean

USER $user
WORKDIR /home/$user
