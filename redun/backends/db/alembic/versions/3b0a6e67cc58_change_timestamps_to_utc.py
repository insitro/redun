"""change timestamps to utc

Revision ID: 3b0a6e67cc58
Revises: f68b3aaee9cc
Create Date: 2024-08-24 14:20:17.063050

"""

import os

from alembic import op


# revision identifiers, used by Alembic.
revision = "3b0a6e67cc58"
down_revision = "f68b3aaee9cc"
branch_labels = None
depends_on = None


def upgrade():
    if op.get_bind().dialect.name == "postgresql":
        # Users can define what local timezone should be assumed for their
        # database using this environment variable.
        local_timezone = os.environ.get("REDUN_LOCALTIMEZONE", "UTC")

        op.execute(
            f"""
            alter table job
            alter column start_time type timestamp with time zone;

            alter table job
            alter column end_time type timestamp with time zone;

            alter table call_node
            alter column "timestamp" type timestamp with time zone;

            alter table redun_version
            alter column "timestamp" type timestamp with time zone;

            -- Create trigger that only runs for old clients to help convert their timestamps.
            CREATE OR REPLACE FUNCTION check_job_utc()
            RETURNS TRIGGER AS $$
            BEGIN
                IF current_setting('redun.version', true) IS NULL THEN
                    -- For older redun clients, convert time from local timezone to UTC.
                    set local timezone = '{local_timezone}';
                    NEW.start_time := NEW.start_time AT TIME ZONE 'UTC';
                    NEW.end_time := NEW.end_time AT TIME ZONE 'UTC';
                END IF;

                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;


            DROP TRIGGER IF EXISTS check_job_utc ON job;
            CREATE TRIGGER check_job_utc
            BEFORE INSERT ON job
            FOR EACH ROW
            EXECUTE FUNCTION check_job_utc();
            """
        )

    elif op.get_bind().dialect.name == "sqlite":
        op.execute(
            """
            update job set
              start_time = datetime(start_time, 'utc'),
              end_time = datetime(end_time, 'utc');
            """
        )


def downgrade():
    if op.get_bind().dialect.name == "postgresql":
        op.execute(
            """
            alter table job
            alter column start_time type timestamp without time zone;

            alter table job
            alter column end_time type timestamp without time zone;

            alter table call_node
            alter column "timestamp" type timestamp without time zone;

            alter table redun_version
            alter column "timestamp" type timestamp without time zone;

            DROP TRIGGER IF EXISTS check_job_utc ON job;
            DROP FUNCTION IF EXISTS check_job_utc;
            """
        )

    elif op.get_bind().dialect.name == "sqlite":
        op.execute(
            """
            update job set
              start_time = datetime(start_time, 'localtime'),
              end_time = datetime(end_time, 'localtime');
            """
        )
