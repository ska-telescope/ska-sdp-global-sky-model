"""
Database testing utils
"""

import hashlib
import logging
from time import sleep

from sqlalchemy import JSON, StaticPool, create_engine, event
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import sessionmaker

from ska_sdp_global_sky_model.api.app.models import (
    GlobalSkyModelMetadata,
    SkyComponent,
    SkyComponentStaging,
)
from ska_sdp_global_sky_model.configuration.config import Base

logger = logging.getLogger(__name__)

# Use in-memory SQLite for testing
SQLALCHEMY_DATABASE_URL = "sqlite:///:memory:"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,  # Use StaticPool to keep a single connection
)

TESTING_SESSION_LOCAL = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def override_get_db():
    """
    Create a local testing session on demand.
    """
    try:
        db = TESTING_SESSION_LOCAL()
        yield db
    finally:
        db.close()


@event.listens_for(engine, "connect")
def register_q3c_mock(dbapi_conn, connection_record):  # pylint: disable=unused-argument
    """Register a mock Q3C function for SQLite."""

    def q3c_radial_query_mock(ra1, dec1, ra2, dec2, radius):
        """Mock Q3C function that does a simple box check instead of proper spherical distance."""
        # Simple box check - not accurate but sufficient for testing
        ra_diff = abs(ra1 - ra2)
        dec_diff = abs(dec1 - dec2)
        # Treat radius as degrees and check if point is within box
        return 1 if (ra_diff <= radius and dec_diff <= radius) else 0

    dbapi_conn.create_function("q3c_radial_query", 5, q3c_radial_query_mock)


# pylint: disable-next=no-member
@event.listens_for(Base.metadata, "before_create")
def replace_jsonb_sqlite(target, connection, **kw):  # pylint: disable=unused-argument
    """Replace JSONB with JSON and remove schema for SQLite."""
    if connection.dialect.name == "sqlite":
        for table in target.tables.values():
            table.schema = None
            for column in table.columns:
                if isinstance(column.type, JSONB):
                    column.type = JSON()


def set_up_db():
    """
    Add testing data to database.

    component_ids in catalogues start with:
    - catalogue1-Alice: W000010
    - catalogue1-Bob: X000020
    - catalogue2: A000100
    - catalogue3: L000105
    total number of components in db: 250
    """
    db = next(override_get_db())

    try:
        _generate_catalogue(db, "catalogue1", "0.1.0", "Alice", 76e6, 100e6, (90, 2), 20)
        _generate_catalogue(db, "catalogue1", "0.2.0", "Bob", 50e6, 110e6, (90, 4), 10)
        _generate_catalogue(db, "catalogue3", "1.0.5", "SKA SDP Team", 50e6, 350e6, (80, 4), 20)
        # upload the next slightly later than others so we can test based on
        # upload time else the upload time is the same for all of them
        sleep(1)
        _generate_catalogue(db, "catalogue2", "1.0.0", "A.N. Other", 150e6, 227e6, (70, 4), 200)

    finally:
        db.close()


# pylint: disable-next=too-many-arguments,too-many-positional-arguments,too-many-locals
def _generate_catalogue(
    db,
    name: str,
    version: str,
    author: str,
    freq_min_hz: float,
    freq_max_hz: float,
    mid: tuple[float, float],
    count: int,
):
    """
    component_ids in catalogues start with:
    - catalogue1-Alice: W000010
    - catalogue1-Bob: X000020
    - catalogue2: A000100
    - catalogue3: N000100

    fluxes for each catalogue: 0.1--N*0.1, where N=count
    position angles: 0--(N-1), where N=count
    reference frequency per component: (freq_min_hz + freq_max_hz) / 2
    """
    upload_id = hashlib.sha1(f"{name}-{version}".encode("utf-8")).hexdigest()
    code = chr(65 + sum(ord(x) for x in upload_id) % 26)
    logger.info(
        "%s - %s - %s - %s - (%f, %f) - %d", name, version, upload_id, code, mid[0], mid[1], count
    )

    metadata = GlobalSkyModelMetadata(
        version=version,
        catalogue_name=name,
        author=author,
        freq_min_hz=freq_min_hz,
        freq_max_hz=freq_max_hz,
        upload_id=upload_id,
    )
    db.add(metadata)
    db.commit()

    mr, md = mid[0] - count / 10, mid[1] - count / 10

    for i in range(count):
        r, d = mr + i / 5, md + i / 5
        db.add(
            SkyComponent(
                component_id=f"{code}{version.replace('.', ''):0>6}+{i:0>6}",
                ra_deg=r,
                dec_deg=d,
                gsm_id=metadata.id,
                pa_deg=i,
                i_pol_jy=0.1 * (i + 1),
                ref_freq_hz=(freq_min_hz + freq_max_hz) / 2,
            )
        )
    db.commit()


def clean_all_tables():
    """Clean staging, main sky component, and catalogue metadata tables for test isolation."""
    db = next(override_get_db())
    try:
        db.query(GlobalSkyModelMetadata).delete()
        db.query(SkyComponentStaging).delete()
        db.query(SkyComponent).delete()
        db.query(GlobalSkyModelMetadata).delete()
        db.commit()
    finally:
        db.close()
