# Development
FROM ubuntu:16.04

# Install environment
RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys D82D8E35
RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys EAA542E8
RUN echo "deb https://repo.evernym.com/deb xenial master" >> /etc/apt/sources.list
RUN echo "deb https://repo.sovrin.org/deb xenial master" >> /etc/apt/sources.list
RUN apt-get remove -y appstream
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