#!/bin/bash
set -ex
export DOCKER_IMG=redun_python_test:redun_python_test
export DOCKER_CONTAINER_NAME=redun_python_test

docker build . --tag $DOCKER_IMG >/dev/null
docker-compose up -d

( docker rm -f $DOCKER_CONTAINER_NAME || true ) 2>&1 >/dev/null

if [ -t 0 ]; then
        TERM="-i -t"
else
        TERM="-i "
fi

docker run \
         -u "$(id -u):$(id -g)" \
        $TERM \
        --rm \
        --name $DOCKER_CONTAINER_NAME \
        -e PGUSER=postgres -e PGPASSWORD=postgres \
        -v "$PWD:/v" \
        -w /v \
        --net host \
        --shm-size=4gb \
        --memory 6gb \
        --memory-swap 6gb \
        $DOCKER_IMG \
        "$@"
