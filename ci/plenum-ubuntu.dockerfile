# Development
FROM ubuntu:16.04

# Install environment
ADD ci/orientdb.deb /var/cache/apt/archives/orientdb.deb
RUN apt-get update -y
RUN apt-get install -y \ 
	git \
	wget \
	python3.5 \
	python3-pip \
	python-setuptools \
	orientdb
RUN pip3 install -U \ 
	pip \ 
	setuptools \
	virtualenv
RUN systemctl start orientdb