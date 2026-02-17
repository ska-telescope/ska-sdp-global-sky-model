"""Add catalog_metadata table

Revision ID: 2bb60f51271b
Revises: 5d5a9c279421
Create Date: 2026-02-13 09:30:00.000000

"""
from typing import Sequence

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '2bb60f51271b'
down_revision: str | None = '5d5a9c279421'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create catalog_metadata table for catalog-level versioning."""
    op.create_table(
        'catalog_metadata',
        sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column('version', sa.String(), nullable=False),
        sa.Column('catalogue_name', sa.String(), nullable=False),
        sa.Column('description', sa.String(), nullable=True),
        sa.Column('upload_id', sa.String(), nullable=False),
        sa.Column(
            'uploaded_at',
            sa.DateTime(),
            nullable=False,
            server_default=sa.text('now()')
        ),
        sa.Column('ref_freq', sa.Float(), nullable=False, comment='Reference frequency in Hz'),
        sa.Column('epoch', sa.String(), nullable=False, comment='Epoch of observation'),
        sa.Column('author', sa.String(), nullable=True),
        sa.Column('reference', sa.String(), nullable=True),
        sa.Column('notes', sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        schema='public'
    )
    
    # Create indexes
    op.create_index(
        op.f('ix_public_catalog_metadata_version'),
        'catalog_metadata',
        ['version'],
        unique=True,
        schema='public'
    )
    op.create_index(
        op.f('ix_public_catalog_metadata_upload_id'),
        'catalog_metadata',
        ['upload_id'],
        unique=True,
        schema='public'
    )


def downgrade() -> None:
    """Drop catalog_metadata table and indexes."""
    op.drop_index(
        op.f('ix_public_catalog_metadata_upload_id'),
        table_name='catalog_metadata',
        schema='public'
    )
    op.drop_index(
        op.f('ix_public_catalog_metadata_version'),
        table_name='catalog_metadata',
        schema='public'
    )
    op.drop_table('catalog_metadata', schema='public')
