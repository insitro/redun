FROM ubuntu:20.04

# Install OS packages.
RUN apt-get update && DEBIAN_FRONTEND="noninteractive" apt-get install -y \
  bzip2 \
  curl \
  default-jre \
  python \
  python3 \
  python3-pip \
  r-base \
  tzdata \
  zip && \
  apt-get clean


# Install Python packages.
RUN pip3 install \
  awscli \
  cutadapt==2.4

RUN mkdir /software

# Install BWA.
RUN cd /software/ && curl -L https://github.com/bwa-mem2/bwa-mem2/releases/download/v2.2.1/bwa-mem2-2.2.1_x64-linux.tar.bz2 | tar -jxf -

# Install GATK.
RUN cd /software/ && curl -L https://github.com/broadinstitute/gatk/releases/download/4.2.0.0/gatk-4.2.0.0.zip > gatk.zip && unzip gatk.zip && rm gatk.zip

# Install Picard.
RUN cd /software/ && curl -L https://github.com/broadinstitute/picard/releases/download/2.25.6/picard.jar > picard.jar

# Install samtools.
RUN apt-get install -y samtools

WORKDIR /root/
ENV PATH="$PATH:/software/bwa-mem2-2.2.1_x64-linux/:/software/gatk-4.2.0.0"
