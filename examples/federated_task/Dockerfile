FROM ubuntu:24.04

# Install OS-level libraries.
RUN apt-get update -y && DEBIAN_FRONTEND="noninteractive" apt-get install -y \
    python3 \
    python3-pip && \
    apt-get clean

WORKDIR /code

# Install our python code dependencies.
# Here we copy the redun library into the container, but we could also
# use `pip install redun`.
COPY examples/federated_task/remote_repo remote_repo
COPY . redun
RUN pip3 install --break-system-packages ./redun
