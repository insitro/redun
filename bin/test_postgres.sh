#!/usr/bin/env bash

# Start database.
which docker-compose && docker-compose up -d || docker compose up -d

# Wait for database to be ready.
until psql postgresql://postgres:postgres@localhost:5432/postgres -c "select 1" > /dev/null 2>&1; do
    sleep 1;
done

# Run tests.
REDUN_TEST_POSTGRES=1 uv run pytest redun $TEST_ARGS
EXIT_CODE=$?

# Shutdown database.
which docker-compose && docker-compose down || docker compose down
exit $EXIT_CODE
