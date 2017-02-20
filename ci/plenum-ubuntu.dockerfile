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
	wget
RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys D82D8E35
RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys EAA542E8
RUN echo "deb https://repo.evernym.com/deb xenial master" >> /etc/apt/sources.list
RUN echo "deb https://repo.sovrin.org/deb xenial master" >> /etc/apt/sources.list
RUN mkdir /tmp/asfix
RUN cd /tmp/asfix && wget https://launchpad.net/ubuntu/+archive/primary/+files/appstream_0.9.4-1ubuntu1_amd64.deb
RUN cd /tmp/asfix && wget https://launchpad.net/ubuntu/+archive/primary/+files/libappstream3_0.9.4-1ubuntu1_amd64.deb
RUN cd /tmp/asfix && dpkg -i *.deb
RUN apt-get update -y
RUN apt-get install -y \
	orientdb
RUN pip3 install -U \ 
	pip \ 
	setuptools \
	virtualenv
RUN systemctl start orientdb