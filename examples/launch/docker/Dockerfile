FROM ubuntu:20.04

# Install OS-level libraries.
RUN apt-get update -y && DEBIAN_FRONTEND="noninteractive" apt-get install -y \
    python3 \
    python3-pip && \
    apt-get clean

WORKDIR /code

COPY requirements.txt .
RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt
COPY .redun/ .redun/
COPY workflow.py .
