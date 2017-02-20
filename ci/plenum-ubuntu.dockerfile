# Development
FROM ubuntu:16.04

# Install environment
RUN apt-get update -y
RUN apt-get install -y \ 
	git \
	wget \
	python3.5 \
	python3-pip \
	python-setuptools \
	gdebi-core
RUN pip3 install -U \ 
	pip \ 
	setuptools \
	virtualenv
ADD ci/orientdb.deb orientdb.deb
RUN gdebi orientdb.deb
RUN systemctl start orientdb