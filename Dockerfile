FROM centos:8
RUN dnf install -y python3
COPY . /packtivity
WORKDIR /packtivity
RUN dnf install -y python3-pip
RUN pip3 install -e .
