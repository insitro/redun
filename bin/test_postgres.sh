#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. $DIR/../.venv/bin/activate

# Start database.
which docker-compose && docker-compose up -d || docker compose up -d

# Wait for database to be ready.
until psql postgresql://postgres:postgres@localhost:5432/postgres -c "select 1" > /dev/null 2>&1; do
    sleep 1;
done

# Run tests.
REDUN_TEST_POSTGRES=1 pytest redun $TEST_ARGS
EXIT_CODE=$?

# Shutdown database.
which docker-compose && docker-compose down || docker compose down
exit $EXIT_CODE
