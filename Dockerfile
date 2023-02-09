FROM almalinux:9.1

COPY . /packtivity
WORKDIR /packtivity

# Set PATH to pickup virtualenv by default
ENV PATH=/usr/local/venv/bin:"${PATH}"
RUN dnf install -y \
        python3 \
        python3-pip && \
    python3 -m venv /usr/local/venv && \
    . /usr/local/venv/bin/activate && \
    python -m pip --no-cache-dir install --upgrade pip setuptools wheel && \
    python -m pip --no-cache-dir install . && \
    python -m pip list
