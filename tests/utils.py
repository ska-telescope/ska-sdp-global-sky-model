"""
Database testing utils
"""

from sqlalchemy import JSON, StaticPool, create_engine, event
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import sessionmaker

from ska_sdp_global_sky_model.api.app.models import SkyComponent, SkyComponentStaging
from ska_sdp_global_sky_model.configuration.config import Base

# Use in-memory SQLite for testing
SQLALCHEMY_DATABASE_URL = "sqlite:///:memory:"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,  # Use StaticPool to keep a single connection
)


def override_get_db():
    """
    Create a local testing session.
    """
    try:
        db = TESTING_SESSION_LOCAL()
        yield db
    finally:
        db.close()


TESTING_SESSION_LOCAL = sessionmaker(autocommit=False, autoflush=False, bind=engine)


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


def clean_all_tables():
    """Clean both staging and main tables for test isolation."""
    db = next(override_get_db())
    try:
        db.query(SkyComponentStaging).delete()
        db.query(SkyComponent).delete()
        db.commit()
    finally:
        db.close()
