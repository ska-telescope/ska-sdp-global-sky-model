"""re-index

Revision ID: 524a45068400
Revises: 878dccc7d809
Create Date: 2026-02-03 08:48:00.516198

"""
from typing import Sequence

from alembic import op
from sqlalchemy.sql import text


# revision identifiers, used by Alembic.
revision: str = '524a45068400'
down_revision: str | None = '878dccc7d809'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(text("DROP INDEX IF EXISTS idx_source_q3c_ipix;"))
    op.execute(text("CREATE EXTENSION IF NOT EXISTS q3c;"))
    op.execute(
        text(
            "CREATE INDEX IF NOT EXISTS idx_source_q3c_ipix "
            'ON source (q3c_ang2ipix("ra","dec"));'
        )
    )

def downgrade() -> None:
    pass

