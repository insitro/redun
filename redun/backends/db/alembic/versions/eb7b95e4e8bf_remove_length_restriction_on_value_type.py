"""Remove length restriction on value type

Revision ID: eb7b95e4e8bf
Revises: cc4f663817b6
Create Date: 2022-11-07 16:22:50.409259

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "eb7b95e4e8bf"
down_revision = "cc4f663817b6"
branch_labels = None
depends_on = None


def upgrade():
    if op.get_bind().dialect.name != "sqlite":
        op.alter_column(
            "value",
            "type",
            existing_type=sa.String(length=100),
            type_=sa.String(length=None),
        )


def downgrade():
    if op.get_bind().dialect.name != "sqlite":
        op.alter_column(
            "value",
            "type",
            existing_type=sa.String(length=None),
            type_=sa.String(length=100),
        )
