FROM ubuntu:24.04

# Install OS-level libraries.
RUN apt-get update -y && DEBIAN_FRONTEND="noninteractive" apt-get install -y \
    python3 \
    python3-pip \
    wget && \
    apt-get clean

WORKDIR /code

# Install conda.
RUN wget https://repo.anaconda.com/miniconda/Miniconda3-py38_23.3.1-0-Linux-x86_64.sh
RUN bash Miniconda3-py38_23.3.1-0-Linux-x86_64.sh -b

# Install conda environment.
COPY environment.yml .
RUN /root/miniconda3/bin/conda env create --name my-env -vv

# Run command inside conda environment using docker entrypoint.
ENTRYPOINT ["/root/miniconda3/bin/conda", "run", "-n", "my-env"]
