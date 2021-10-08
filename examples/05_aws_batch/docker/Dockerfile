
# Chose any base image you would like:
ARG REGISTRY
FROM ${REGISTRY}/amazonlinux-python3
WORKDIR /code

# Install OS-level libraries.
RUN yum update -y && yum install -y \
    postgresql-devel

# Install our python code dependencies.
COPY redun redun
COPY requirements.txt .
RUN pip install -r requirements.txt
