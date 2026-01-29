# flake8: noqa
# pylint: disable=all
"""q3c_index

Revision ID: 624b22e2fd6a
Revises: 6ab7861d2188
Create Date: 2026-01-23 09:11:00.835022

"""

from typing import Sequence

from alembic import op
from sqlalchemy.sql import text

# revision identifiers, used by Alembic.
revision: str = "624b22e2fd6a"
down_revision: str | None = "6ab7861d2188"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(text("CREATE EXTENSION IF NOT EXISTS q3c;"))
    op.execute(
        text(
            "CREATE INDEX IF NOT EXISTS idx_source_q3c_ipix "
            'ON source (q3c_ang2ipix("RAJ2000","DECJ2000"));'
        )
    )


def downgrade() -> None:
    """
    No downgrade path provided.
    """
    pass
