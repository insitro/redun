#!/bin/bash
set -e
ARGS="$@"
./.run.docker.sh bash -c "set -ex; export TEST_ARGS=\"$ARGS\"; make test-postgres"
