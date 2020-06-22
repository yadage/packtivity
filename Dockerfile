FROM centos:8
RUN dnf install -y python3 python3-devel gcc autoconf make
COPY . /packtivity
WORKDIR /packtivity
RUN curl https://bootstrap.pypa.io/get-pip.py | python3 -
RUN curl https://get.docker.com/builds/Linux/x86_64/docker-1.9.1  -o /usr/bin/docker && chmod +x /usr/bin/docker
RUN pip install -e . 
