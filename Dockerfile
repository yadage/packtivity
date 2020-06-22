FROM cern/cc7-base
RUN yum install -y gcc gcc-c++ graphviz-devel ImageMagick python-devel libffi-devel openssl openssl-devel autoconf automake libtool
COPY . /packtivity
WORKDIR /packtivity
RUN curl https://bootstrap.pypa.io/get-pip.py | python -
RUN curl https://get.docker.com/builds/Linux/x86_64/docker-1.9.1  -o /usr/bin/docker && chmod +x /usr/bin/docker
RUN pip install -e . --process-dependency-links
