"""Tests for SQLAlchemy models."""

# pylint: disable=redefined-outer-name, no-member

import datetime

import pytest
from sqlalchemy import JSON, TypeDecorator, create_engine, event
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from ska_sdp_global_sky_model.api.app.model import Base, Source, Version


# Monkey-patch JSONB to work with SQLite for tests
class JSONBCompat(TypeDecorator):
    """JSONB compatibility layer for SQLite tests."""

    impl = JSON
    cache_ok = True


# Replace JSONB with JSON-compatible version for SQLite
@event.listens_for(Base.metadata, "before_create")
def replace_jsonb_sqlite(target, connection, **kw):  # pylint: disable=unused-argument
    """Replace JSONB with JSON and remove schema for SQLite."""
    if connection.dialect.name == "sqlite":
        for table in target.tables.values():
            # Remove schema for SQLite
            table.schema = None
            for column in table.columns:
                if isinstance(column.type, JSONB):
                    column.type = JSON()


@pytest.fixture
def test_engine():
    """Create an in-memory SQLite database for testing."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return engine


@pytest.fixture
def test_session(test_engine):  # pylint: disable=unused-argument
    """Create a test database session."""
    testing_session_local = sessionmaker(autocommit=False, autoflush=False, bind=test_engine)
    session = testing_session_local()
    yield session
    session.close()


class TestVersionModel:  # pylint: disable=too-few-public-methods
    """Tests for the Version model."""

    def test_create_version(self, test_session):
        """Test creating a Version object."""
        version = Version(
            layer_id="test_catalog",
            version="1.0.0",
            epoch=datetime.datetime(2024, 1, 1),
            date_added=datetime.datetime.now(),
            default_version=True,
        )
        test_session.add(version)
        test_session.commit()

        assert version.id is not None
        assert version.layer_id == "test_catalog"
        assert version.version == "1.0.0"
        assert version.default_version is True

    def test_version_repr(self, test_session):
        """Test Version __repr__ method."""
        version = Version(
            layer_id="gleam",
            version="2.0.0",
            default_version=False,
        )
        test_session.add(version)
        test_session.commit()

        repr_str = repr(version)
        assert "Version" in repr_str
        assert "gleam" in repr_str
        assert "2.0.0" in repr_str

    def test_unique_constraint_layer_version(self, test_session):
        """Test that layer_id + version must be unique."""
        version1 = Version(layer_id="test", version="1.0.0")
        test_session.add(version1)
        test_session.commit()

        # Try to add duplicate
        version2 = Version(layer_id="test", version="1.0.0")
        test_session.add(version2)

        with pytest.raises(IntegrityError):
            test_session.commit()

    def test_version_with_metadata(self, test_session):
        """Test Version with JSONB metadata."""
        metadata = {
            "telescope": "MWA",
            "frequency_range": [70, 230],
            "description": "Test catalog",
        }
        version = Version(
            layer_id="test",
            version="1.0.0",
            catalogue_metadata=metadata,
        )
        test_session.add(version)
        test_session.commit()

        retrieved = test_session.query(Version).filter_by(layer_id="test").first()
        assert retrieved.catalogue_metadata == metadata


class TestSourceModel:  # pylint: disable=too-few-public-methods
    """Tests for the Source model."""

    def test_create_source(self, test_session):
        """Test creating a Source object."""
        # First create a version
        version = Version(layer_id="test", version="1.0.0")
        test_session.add(version)
        test_session.commit()

        # Create source
        source = Source(
            name="TEST_J123456+123456",
            RAJ2000=123.456,
            DECJ2000=12.3456,
            Heal_Pix_Position=12345,
            version_id=version.id,
        )
        test_session.add(source)
        test_session.commit()

        assert source.id is not None
        assert source.name == "TEST_J123456+123456"
        assert source.RAJ2000 == 123.456
        assert source.DECJ2000 == 12.3456

    def test_source_repr(self, test_session):
        """Test Source __repr__ method."""
        version = Version(layer_id="test", version="1.0.0")
        test_session.add(version)
        test_session.commit()

        source = Source(
            name="TEST_SOURCE",
            RAJ2000=180.0,
            DECJ2000=0.0,
            Heal_Pix_Position=100,
            version_id=version.id,
        )
        test_session.add(source)
        test_session.commit()

        repr_str = repr(source)
        assert "Source" in repr_str
        assert "TEST_SOURCE" in repr_str
        assert "180.0000" in repr_str

    def test_source_unique_name(self, test_session):
        """Test that source names must be unique."""
        version = Version(layer_id="test", version="1.0.0")
        test_session.add(version)
        test_session.commit()

        source1 = Source(
            name="DUPLICATE_NAME",
            RAJ2000=100.0,
            DECJ2000=20.0,
            Heal_Pix_Position=100,
            version_id=version.id,
        )
        test_session.add(source1)
        test_session.commit()

        # Try to add duplicate name
        source2 = Source(
            name="DUPLICATE_NAME",
            RAJ2000=200.0,
            DECJ2000=30.0,
            Heal_Pix_Position=200,
            version_id=version.id,
        )
        test_session.add(source2)

        with pytest.raises(IntegrityError):
            test_session.commit()

    def test_source_version_relationship(self, test_session):
        """Test the relationship between Source and Version."""
        version = Version(layer_id="test", version="1.0.0")
        test_session.add(version)
        test_session.commit()

        source = Source(
            name="TEST_SOURCE",
            RAJ2000=150.0,
            DECJ2000=-30.0,
            Heal_Pix_Position=500,
            version_id=version.id,
        )
        test_session.add(source)
        test_session.commit()

        # Test forward relationship
        assert source.version_ref is not None
        assert source.version_ref.id == version.id
        assert source.version_ref.layer_id == "test"

        # Test back-reference
        assert len(version.sources) == 1
        assert version.sources[0].name == "TEST_SOURCE"

    def test_source_with_frequency_data(self, test_session):
        """Test Source with GLEAM frequency-specific columns."""
        version = Version(layer_id="gleam", version="1.0.0")
        test_session.add(version)
        test_session.commit()

        source = Source(
            name="GLEAM_SOURCE",
            RAJ2000=200.0,
            DECJ2000=-45.0,
            Heal_Pix_Position=1000,
            version_id=version.id,
            I_76=1.23,
            MajorAxis_76=15.5,
            MinorAxis_76=12.3,
            Orientation_76=45.0,
        )
        test_session.add(source)
        test_session.commit()

        retrieved = test_session.query(Source).filter_by(name="GLEAM_SOURCE").first()
        assert retrieved.I_76 == 1.23
        assert retrieved.MajorAxis_76 == 15.5
        assert retrieved.MinorAxis_76 == 12.3
        assert retrieved.Orientation_76 == 45.0

    def test_source_nullable_frequency_columns(self, test_session):
        """Test that frequency-specific columns can be NULL."""
        version = Version(layer_id="test", version="1.0.0")
        test_session.add(version)
        test_session.commit()

        # Create source without frequency data
        source = Source(
            name="MINIMAL_SOURCE",
            RAJ2000=100.0,
            DECJ2000=10.0,
            Heal_Pix_Position=123,
            version_id=version.id,
        )
        test_session.add(source)
        test_session.commit()

        retrieved = test_session.query(Source).filter_by(name="MINIMAL_SOURCE").first()
        assert retrieved.I_76 is None
        assert retrieved.MajorAxis_76 is None


class TestModelRelationships:  # pylint: disable=too-few-public-methods
    """Tests for relationships between models."""

    def test_cascade_version_sources(self, test_session):
        """Test that sources are accessible from version."""
        version = Version(layer_id="test", version="1.0.0")
        test_session.add(version)
        test_session.commit()

        # Create multiple sources
        for i in range(3):
            source = Source(
                name=f"SOURCE_{i}",
                RAJ2000=100.0 + i,
                DECJ2000=20.0 + i,
                Heal_Pix_Position=100 + i,
                version_id=version.id,
            )
            test_session.add(source)
        test_session.commit()

        # Retrieve version and check sources
        retrieved_version = test_session.query(Version).filter_by(layer_id="test").first()
        assert len(retrieved_version.sources) == 3
        assert all(s.version_ref == retrieved_version for s in retrieved_version.sources)
