FROM python:3.10-bookworm
ENV DEBIAN_FRONTEND=noninteractive
RUN mkdir -p /usr/share/man/man1 /usr/share/man/man2
RUN apt-get update -y && apt-get install -y libpq-dev gcc   graphviz graphviz-dev  openjdk-17-jdk  libpq-dev gcc postgresql-client-15 pre-commit
RUN java --version
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64/
ENV REQUIRE_POSTGRES=1

# run containers with user with id 1000
RUN useradd -rm -d /home/debian -s /bin/bash -g root -G sudo -u 1000 debian
USER debian
WORKDIR /home/debian
