FROM ubuntu:20.04

# Install OS-level libraries.
RUN apt-get update -y && DEBIAN_FRONTEND="noninteractive" apt-get install -y \
    python3 \
    python3-pip && \
    apt-get clean

WORKDIR /code

# Install our python code dependencies.
# Here we copy the redun library into the container, but we could also
# use `pip install redun`.
COPY redun redun
COPY requirements.txt .
RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt
