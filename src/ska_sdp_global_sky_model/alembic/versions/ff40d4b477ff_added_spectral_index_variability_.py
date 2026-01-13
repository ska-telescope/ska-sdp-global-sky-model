# pylint: disable=E1101(no-member)
"""
Added spectral index, variability, polarisation.
Revision ID: ff40d4b477ff.
Revises: .
Create Date: 2024-08-20 10:28:02.489540.
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "ff40d4b477ff"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Migration script for updating models."""
    op.drop_table("field")
    op.drop_index(
        "ix_sdp_sdp_global_sky_model_integration_fieldtile_hpx",
        table_name="fieldtile",
        postgresql_using="spgist",
    )
    op.drop_table("fieldtile")
    op.drop_constraint("band_telescope_fkey", "band", type_="foreignkey")
    op.create_foreign_key(
        None,
        "band",
        "telescope",
        ["telescope"],
        ["id"],
        source_schema="sdp_sdp_global_sky_model_integration",
        referent_schema="sdp_sdp_global_sky_model_integration",
    )
    op.add_column("narrowbanddata", sa.Column("Polarised", sa.Boolean(), nullable=True))
    op.add_column("narrowbanddata", sa.Column("Stokes", sa.String(), nullable=True))
    op.add_column("narrowbanddata", sa.Column("Rotational_Measure", sa.Float(), nullable=True))
    op.add_column(
        "narrowbanddata", sa.Column("Rotational_Measure_Error", sa.Float(), nullable=True)
    )
    op.add_column(
        "narrowbanddata", sa.Column("Fractional_Polarisation", sa.Float(), nullable=True)
    )
    op.add_column(
        "narrowbanddata", sa.Column("Fractional_Polarisation_Error", sa.Float(), nullable=True)
    )
    op.add_column("narrowbanddata", sa.Column("Faraday_Complex", sa.Boolean(), nullable=True))
    op.add_column("narrowbanddata", sa.Column("Spectral_Index", sa.Float(), nullable=True))
    op.add_column("narrowbanddata", sa.Column("Spectral_Index_Error", sa.Float(), nullable=True))
    op.add_column("narrowbanddata", sa.Column("Variable", sa.Boolean(), nullable=True))
    op.add_column("narrowbanddata", sa.Column("Modulation_Index", sa.Float(), nullable=True))
    op.add_column(
        "narrowbanddata", sa.Column("Debiased_Modulation_Index", sa.Float(), nullable=True)
    )
    op.drop_constraint("narrowbanddata_band_fkey", "narrowbanddata", type_="foreignkey")
    op.drop_constraint("narrowbanddata_source_fkey", "narrowbanddata", type_="foreignkey")
    op.create_foreign_key(
        None,
        "narrowbanddata",
        "source",
        ["source"],
        ["id"],
        source_schema="sdp_sdp_global_sky_model_integration",
        referent_schema="sdp_sdp_global_sky_model_integration",
    )
    op.create_foreign_key(
        None,
        "narrowbanddata",
        "band",
        ["band"],
        ["id"],
        source_schema="sdp_sdp_global_sky_model_integration",
        referent_schema="sdp_sdp_global_sky_model_integration",
    )
    op.drop_constraint("skytile_id_fkey", "skytile", type_="foreignkey")
    op.create_foreign_key(
        None,
        "skytile",
        "wholesky",
        ["id"],
        ["id"],
        source_schema="sdp_sdp_global_sky_model_integration",
        referent_schema="sdp_sdp_global_sky_model_integration",
        ondelete="CASCADE",
    )
    op.drop_constraint("source_tile_id_fkey", "source", type_="foreignkey")
    op.create_foreign_key(
        None,
        "source",
        "skytile",
        ["tile_id"],
        ["pk"],
        source_schema="sdp_sdp_global_sky_model_integration",
        referent_schema="sdp_sdp_global_sky_model_integration",
    )
    op.add_column("widebanddata", sa.Column("Polarised", sa.Boolean(), nullable=True))
    op.add_column("widebanddata", sa.Column("Stokes", sa.String(), nullable=True))
    op.add_column("widebanddata", sa.Column("Rotational_Measure", sa.Float(), nullable=True))
    op.add_column("widebanddata", sa.Column("Rotational_Measure_Error", sa.Float(), nullable=True))
    op.add_column("widebanddata", sa.Column("Fractional_Polarisation", sa.Float(), nullable=True))
    op.add_column(
        "widebanddata", sa.Column("Fractional_Polarisation_Error", sa.Float(), nullable=True)
    )
    op.add_column("widebanddata", sa.Column("Faraday_Complex", sa.Boolean(), nullable=True))
    op.add_column("widebanddata", sa.Column("Spectral_Curvature", sa.Float(), nullable=True))
    op.add_column("widebanddata", sa.Column("Spectral_Curvature_Error", sa.Float(), nullable=True))
    op.add_column("widebanddata", sa.Column("Variable", sa.Boolean(), nullable=True))
    op.add_column("widebanddata", sa.Column("Modulation_Index", sa.Float(), nullable=True))
    op.add_column(
        "widebanddata", sa.Column("Debiased_Modulation_Index", sa.Float(), nullable=True)
    )
    op.drop_constraint("widebanddata_source_fkey", "widebanddata", type_="foreignkey")
    op.drop_constraint("widebanddata_telescope_fkey", "widebanddata", type_="foreignkey")
    op.create_foreign_key(
        None,
        "widebanddata",
        "telescope",
        ["telescope"],
        ["id"],
        source_schema="sdp_sdp_global_sky_model_integration",
        referent_schema="sdp_sdp_global_sky_model_integration",
    )
    op.create_foreign_key(
        None,
        "widebanddata",
        "source",
        ["source"],
        ["id"],
        source_schema="sdp_sdp_global_sky_model_integration",
        referent_schema="sdp_sdp_global_sky_model_integration",
    )
    # ### end Alembic commands ###
