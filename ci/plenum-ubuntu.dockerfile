# Development
FROM ubuntu:16.04

# Install environment
RUN apt-get update -y
RUN apt-get install -y \ 
	git \
	wget \
	python3.5 \
	python3-pip \
	python-setuptools
RUN pip3 install -U \ 
	pip \ 
	setuptools \
	virtualenv
ADD ci/orientdb.deb /tmp/orientdb.deb
RUN apt install -y /tmp/orientdb.deb
RUN systemctl start orientdb