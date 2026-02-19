"""
Unit tests for the database models.

These tests validate that the SQLAlchemy models work correctly,
including their methods and field configurations. Both SkyComponent and
GlobalSkyModelMetadata models: dynamically generated columns from dataclasses
with hardcoded database-specific fields and methods.
"""

# pylint: disable=redefined-outer-name,duplicate-code

import pytest
from ska_sdp_datamodels.global_sky_model.global_sky_model import (
    GlobalSkyModelMetadata as GSMMetadataDataclass,
)
from ska_sdp_datamodels.global_sky_model.global_sky_model import (
    SkyComponent,
)
from sqlalchemy import JSON, create_engine, event, inspect
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from ska_sdp_global_sky_model.api.app.models import GlobalSkyModelMetadata
from ska_sdp_global_sky_model.api.app.models import SkyComponent as SkyComponentModel
from ska_sdp_global_sky_model.api.app.models import SkyComponentStaging
from ska_sdp_global_sky_model.configuration.config import Base

# Use in-memory SQLite for testing
SQLALCHEMY_DATABASE_URL = "sqlite:///:memory:"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)


# pylint: disable=unused-argument)
def q3c_radial_query_mock(*args):
    """Mock q3c_radial_query function."""
    return True


@event.listens_for(engine, "connect")
def register_sqlite_functions(dbapi_connection, connection_record):
    """Load the mock function for q3c_radial_query"""
    dbapi_connection.create_function("q3c_radial_query", 5, q3c_radial_query_mock)


# Make JSONB compatible with SQLite for tests
@event.listens_for(Base.metadata, "before_create")  # pylint: disable=no-member
def replace_jsonb_sqlite(target, connection, **kw):  # pylint: disable=unused-argument
    """Replace JSONB with JSON and remove schema for SQLite."""
    if connection.dialect.name == "sqlite":
        for table in target.tables.values():
            table.schema = None
            for column in table.columns:
                if isinstance(column.type, JSONB):
                    column.type = JSON()


TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


@pytest.fixture
def db_session():
    """Create a fresh database session for each test."""
    Base.metadata.create_all(bind=engine)  # pylint: disable=no-member
    session = TestingSessionLocal()
    yield session
    session.close()
    Base.metadata.drop_all(bind=engine)  # pylint: disable=no-member


class TestSkyComponentModel:
    """Tests for the SkyComponent SQLAlchemy model."""

    def test_sky_component_table_exists(self):
        """Test that SkyComponent table can be created."""
        Base.metadata.create_all(bind=engine)  # pylint: disable=no-member
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        assert "sky_component" in tables or any("sky_component" in t.lower() for t in tables)
        Base.metadata.drop_all(bind=engine)  # pylint: disable=no-member

    def test_sky_component_has_required_columns(self):
        """Test that SkyComponent model has all required columns."""
        Base.metadata.create_all(bind=engine)  # pylint: disable=no-member
        inspector = inspect(engine)

        # Get column names (handle both with and without schema)
        columns = []
        for table_name in inspector.get_table_names():
            if "sky_component" in table_name.lower():
                columns = [col["name"] for col in inspector.get_columns(table_name)]
                break

        # Check for essential columns
        assert "id" in columns
        assert "component_id" in columns
        assert "ra" in columns
        assert "dec" in columns
        assert "i_pol" in columns
        assert "healpix_index" in columns

        Base.metadata.drop_all(bind=engine)  # pylint: disable=no-member

    def test_sky_component_create_instance(self, db_session):
        """Test creating a SkyComponent instance."""
        component = SkyComponentModel(
            component_id="TestSource1",
            ra=123.45,
            dec=-67.89,
            i_pol=1.23,
            healpix_index=12345,
            version="0.1.0",
        )

        db_session.add(component)
        db_session.commit()

        # Verify it was created
        retrieved = (
            db_session.query(SkyComponentModel).filter_by(component_id="TestSource1").first()
        )
        assert retrieved is not None
        assert retrieved.component_id == "TestSource1"
        assert retrieved.ra == 123.45
        assert retrieved.dec == -67.89
        assert retrieved.i_pol == 1.23
        assert retrieved.healpix_index == 12345

    def test_sky_component_with_optional_fields(self, db_session):
        """Test creating a SkyComponent with optional fields."""
        component = SkyComponentModel(
            component_id="TestSource2",
            ra=45.67,
            dec=12.34,
            i_pol=5.67,
            healpix_index=67890,
            major_ax=0.001,
            minor_ax=0.0005,
            pos_ang=1.57,
            spec_idx=[1.0, -0.5, 0.1],
            log_spec_idx=True,
            q_pol=0.1,
            u_pol=0.2,
            v_pol=0.05,
            version="0.1.0",
        )

        db_session.add(component)
        db_session.commit()

        retrieved = (
            db_session.query(SkyComponentModel).filter_by(component_id="TestSource2").first()
        )
        assert retrieved.major_ax == 0.001
        assert retrieved.minor_ax == 0.0005
        assert retrieved.pos_ang == 1.57
        assert retrieved.spec_idx == [1.0, -0.5, 0.1]
        assert retrieved.log_spec_idx is True
        assert retrieved.q_pol == 0.1
        assert retrieved.u_pol == 0.2
        assert retrieved.v_pol == 0.05

    def test_sky_component_versioning_constraint(self, db_session):
        """Test that duplicate component_id + version raises constraint error."""
        component1 = SkyComponentModel(
            component_id="VersionedSource",
            ra=100.0,
            dec=50.0,
            i_pol=1.0,
            healpix_index=11111,
            version="0.0.0",
        )
        db_session.add(component1)
        db_session.commit()

        # Try to create duplicate component_id + version
        component2 = SkyComponentModel(
            component_id="VersionedSource",
            ra=200.0,
            dec=60.0,
            i_pol=2.0,
            healpix_index=22222,
            version="0.0.0",  # Duplicate version
        )
        db_session.add(component2)

        with pytest.raises(Exception):  # Should raise IntegrityError
            db_session.commit()

    def test_sky_component_columns_to_dict_method(self, db_session):
        """Test the columns_to_dict method."""
        component = SkyComponentModel(
            component_id="DictTestSource",
            ra=111.11,
            dec=-22.22,
            i_pol=3.33,
            healpix_index=33333,
            major_ax=0.002,
            version="0.1.0",
        )
        db_session.add(component)
        db_session.commit()

        # Test columns_to_dict
        component_dict = component.columns_to_dict()

        assert isinstance(component_dict, dict)
        assert component_dict["component_id"] == "DictTestSource"
        assert component_dict["ra"] == 111.11
        assert component_dict["dec"] == -22.22
        assert component_dict["i_pol"] == 3.33
        assert component_dict["healpix_index"] == 33333
        assert component_dict["major_ax"] == 0.002
        assert "id" in component_dict

    def test_sky_component_nullable_fields(self, db_session):
        """Test that nullable fields can be None."""
        component = SkyComponentModel(
            component_id="NullTestSource",
            ra=180.0,
            dec=0.0,
            i_pol=1.0,
            healpix_index=55555,
            version="0.1.0",
            # All optional fields left as None
        )
        db_session.add(component)
        db_session.commit()

        retrieved = (
            db_session.query(SkyComponentModel).filter_by(component_id="NullTestSource").first()
        )
        assert retrieved.major_ax is None
        assert retrieved.minor_ax is None
        assert retrieved.pos_ang is None
        assert retrieved.spec_idx is None
        assert retrieved.log_spec_idx is None
        assert retrieved.q_pol is None
        assert retrieved.u_pol is None
        assert retrieved.v_pol is None

    def test_sky_component_spec_idx_as_json(self, db_session):
        """Test that spec_idx field properly stores JSON data."""
        spec_idx_values = [1.5, -0.7, 0.2, -0.05, 0.01]
        component = SkyComponentModel(
            component_id="SpecIdxSource",
            ra=90.0,
            dec=45.0,
            i_pol=2.0,
            healpix_index=66666,
            spec_idx=spec_idx_values,
            version="0.1.0",
        )
        db_session.add(component)
        db_session.commit()

        retrieved = (
            db_session.query(SkyComponentModel).filter_by(component_id="SpecIdxSource").first()
        )
        assert retrieved.spec_idx == spec_idx_values
        assert isinstance(retrieved.spec_idx, list)
        assert len(retrieved.spec_idx) == 5

    def test_sky_component_versioning(self, db_session):
        """Test versioning support, same component_id can have multiple versions."""
        # Create two versions of the same component
        component_v1 = SkyComponentModel(
            component_id="VersionedSource",
            ra=100.0,
            dec=50.0,
            i_pol=1.0,
            healpix_index=11111,
            version="0.0.0",
        )
        component_v2 = SkyComponentModel(
            component_id="VersionedSource",
            ra=100.1,
            dec=50.1,
            i_pol=1.1,
            healpix_index=11111,
            version="0.1.0",
        )
        db_session.add_all([component_v1, component_v2])
        db_session.commit()

        # Both versions should exist
        all_versions = (
            db_session.query(SkyComponentModel).filter_by(component_id="VersionedSource").all()
        )
        assert len(all_versions) == 2
        assert {v.version for v in all_versions} == {"0.0.0", "0.1.0"}


class TestSkyComponentStagingModel:
    """Tests for the SkyComponentStaging SQLAlchemy model."""

    def test_staging_basic_functionality(self, db_session):
        """Test basic SkyComponentStaging CRUD operations."""
        staged = SkyComponentStaging(
            component_id="StagedSource1",
            upload_id="test-upload-123",
            ra=123.45,
            dec=-67.89,
            i_pol=1.23,
            healpix_index=12345,
        )
        db_session.add(staged)
        db_session.commit()

        retrieved = (
            db_session.query(SkyComponentStaging)
            .filter_by(component_id="StagedSource1", upload_id="test-upload-123")
            .first()
        )
        assert retrieved is not None
        assert retrieved.component_id == "StagedSource1"
        assert retrieved.upload_id == "test-upload-123"

    def test_staging_allows_same_component_different_uploads(self, db_session):
        """Test that same component_id can exist in different uploads."""
        staged1 = SkyComponentStaging(
            component_id="TestSource",
            upload_id="upload-001",
            ra=100.0,
            dec=50.0,
            i_pol=1.0,
            healpix_index=11111,
        )
        db_session.add(staged1)
        db_session.commit()

        # Same component_id in different upload - should succeed
        staged2 = SkyComponentStaging(
            component_id="TestSource",
            upload_id="upload-002",
            ra=200.0,
            dec=60.0,
            i_pol=2.0,
            healpix_index=22222,
        )
        db_session.add(staged2)
        db_session.commit()

        # Both should exist
        all_records = (
            db_session.query(SkyComponentStaging).filter_by(component_id="TestSource").all()
        )
        assert len(all_records) == 2

    def test_staging_unique_constraint_violation(self, db_session):
        """Test that duplicate component_id + upload_id raises constraint error."""
        staged1 = SkyComponentStaging(
            component_id="TestSource",
            upload_id="upload-001",
            ra=100.0,
            dec=50.0,
            i_pol=1.0,
            healpix_index=11111,
        )
        db_session.add(staged1)
        db_session.commit()

        # Duplicate component_id + upload_id - should fail
        staged2 = SkyComponentStaging(
            component_id="TestSource",
            upload_id="upload-001",
            ra=200.0,
            dec=60.0,
            i_pol=2.0,
            healpix_index=22222,
        )
        db_session.add(staged2)
        with pytest.raises(Exception):
            db_session.commit()


class TestGlobalSkyModelMetadataModel:
    """Tests for the GlobalSkyModelMetadata SQLAlchemy model."""

    def test_metadata_table_exists(self):
        """Test that GlobalSkyModelMetadata table can be created."""
        Base.metadata.create_all(bind=engine)  # pylint: disable=no-member
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        assert "global_sky_model_metadata" in tables or any(
            "global_sky_model_metadata" in t.lower() for t in tables
        )
        Base.metadata.drop_all(bind=engine)  # pylint: disable=no-member

    def test_metadata_has_required_columns(self):
        """Test that GlobalSkyModelMetadata model has all required columns."""
        Base.metadata.create_all(bind=engine)  # pylint: disable=no-member
        inspector = inspect(engine)

        columns = []
        for table_name in inspector.get_table_names():
            if "global_sky_model_metadata" in table_name.lower():
                columns = [col["name"] for col in inspector.get_columns(table_name)]
                break

        assert "id" in columns
        assert "version" in columns
        assert "ref_freq" in columns
        assert "epoch" in columns

        Base.metadata.drop_all(bind=engine)  # pylint: disable=no-member

    def test_metadata_create_instance(self, db_session):
        """Test creating a GlobalSkyModelMetadata instance."""
        metadata = GlobalSkyModelMetadata(
            version="1.0.0",
            ref_freq=1.4e9,
            epoch="J2000",
            catalogue_name="TestCatalog",
            upload_id="test-upload-1",
        )

        db_session.add(metadata)
        db_session.commit()

        retrieved = db_session.query(GlobalSkyModelMetadata).filter_by(version="1.0.0").first()
        assert retrieved is not None
        assert retrieved.version == "1.0.0"
        assert retrieved.ref_freq == 1.4e9
        assert retrieved.epoch == "J2000"

    def test_metadata_columns_to_dict_method(self, db_session):
        """Test the columns_to_dict method for GlobalSkyModelMetadata."""
        metadata = GlobalSkyModelMetadata(
            version="2.1.0",
            ref_freq=3e9,
            epoch="J2015.5",
            catalogue_name="TestCatalog",
            upload_id="test-upload-2",
        )
        db_session.add(metadata)
        db_session.commit()

        metadata_dict = metadata.columns_to_dict()

        assert isinstance(metadata_dict, dict)
        assert metadata_dict["version"] == "2.1.0"
        assert metadata_dict["ref_freq"] == 3e9
        assert metadata_dict["epoch"] == "J2015.5"
        assert "id" in metadata_dict

    def test_metadata_multiple_versions(self, db_session):
        """Test storing multiple metadata versions."""
        metadata1 = GlobalSkyModelMetadata(
            version="1.0.0",
            ref_freq=1.4e9,
            epoch="J2000",
            catalogue_name="TestCatalog1",
            upload_id="test-upload-1",
        )
        metadata2 = GlobalSkyModelMetadata(
            version="2.0.0",
            ref_freq=1.4e9,
            epoch="J2000",
            catalogue_name="TestCatalog2",
            upload_id="test-upload-2",
        )
        metadata3 = GlobalSkyModelMetadata(
            version="3.0.0",
            ref_freq=3.0e9,
            epoch="J2015",
            catalogue_name="TestCatalog3",
            upload_id="test-upload-3",
        )

        db_session.add_all([metadata1, metadata2, metadata3])
        db_session.commit()

        # Query all metadata entries
        all_metadata = db_session.query(GlobalSkyModelMetadata).all()
        assert len(all_metadata) == 3

        versions = {m.version for m in all_metadata}
        assert versions == {"1.0.0", "2.0.0", "3.0.0"}


class TestModelIntegration:  # pylint: disable=too-few-public-methods
    """Integration tests for models working together."""

    def test_sky_component_and_metadata_coexist(self, db_session):
        """Test that SkyComponent and Metadata can both be stored in the same database."""
        # Create metadata
        metadata = GlobalSkyModelMetadata(
            version="1.0.0",
            ref_freq=1.4e9,
            epoch="J2000",
            catalogue_name="TestCatalog",
            upload_id="test-upload-1",
        )
        db_session.add(metadata)

        # Create components
        component1 = SkyComponentModel(
            component_id="IntegrationSource1",
            ra=100.0,
            dec=50.0,
            i_pol=1.5,
            healpix_index=77777,
            version="0.1.0",
        )
        component2 = SkyComponentModel(
            component_id="IntegrationSource2",
            ra=200.0,
            dec=-30.0,
            i_pol=2.5,
            healpix_index=88888,
            version="0.1.0",
        )
        db_session.add_all([component1, component2])
        db_session.commit()

        # Verify both types of records exist
        metadata_count = db_session.query(GlobalSkyModelMetadata).count()
        component_count = db_session.query(SkyComponentModel).count()

        assert metadata_count == 1
        assert component_count == 2


class TestSkyComponentModelDataclassSync:
    """Tests to verify SkyComponent model stays in sync with SkyComponent dataclass."""

    def test_all_dataclass_fields_present_in_model(self):
        """Test that all fields from SkyComponent are present in SkyComponentModel."""
        # Get all field names from the dataclass
        dataclass_fields = set(SkyComponent.__annotations__.keys())

        # Get all column names from the SkyComponent model
        inspector = inspect(engine)
        Base.metadata.create_all(bind=engine)  # pylint: disable=no-member

        model_columns = set()
        for table_name in inspector.get_table_names():
            if "sky_component" in table_name.lower():
                model_columns = {col["name"] for col in inspector.get_columns(table_name)}
                break

        Base.metadata.drop_all(bind=engine)  # pylint: disable=no-member

        # Check that all dataclass fields are in the model
        # (excluding 'id' and 'healpix_index' which are database-specific)
        missing_fields = dataclass_fields - model_columns
        assert (
            not missing_fields
        ), f"Fields from SkyComponent dataclass missing in SkyComponentModel: {missing_fields}"

    def test_model_has_only_expected_columns(self):
        """Test that SkyComponentModel doesn't have unexpected columns."""
        # Expected columns: dataclass fields + database-specific fields
        expected_columns = set(SkyComponent.__annotations__.keys())
        expected_columns.update(["id", "healpix_index", "version"])

        # Get actual model columns
        inspector = inspect(engine)
        Base.metadata.create_all(bind=engine)  # pylint: disable=no-member

        actual_columns = set()
        for table_name in inspector.get_table_names():
            if "sky_component" in table_name.lower():
                actual_columns = {col["name"] for col in inspector.get_columns(table_name)}
                break

        Base.metadata.drop_all(bind=engine)  # pylint: disable=no-member

        unexpected_columns = actual_columns - expected_columns
        assert (
            not unexpected_columns
        ), f"Unexpected columns in SkyComponentModel: {unexpected_columns}"

    def test_field_count_matches(self):
        """Test that the number of fields matches expectations."""
        # Expected: all dataclass fields + 3 database-specific (id, healpix_index, version)
        expected_count = len(SkyComponent.__annotations__) + 3

        # Get actual count
        inspector = inspect(engine)
        Base.metadata.create_all(bind=engine)  # pylint: disable=no-member

        actual_count = 0
        for table_name in inspector.get_table_names():
            if "sky_component" in table_name.lower():
                actual_count = len(inspector.get_columns(table_name))
                break

        Base.metadata.drop_all(bind=engine)  # pylint: disable=no-member

        assert (
            actual_count == expected_count
        ), f"Expected {expected_count} columns, but found {actual_count}"


class TestGlobalSkyModelMetadataDataclassSync:
    """Tests to verify GlobalSkyModelMetadata model stays in sync with dataclass."""

    def test_all_dataclass_fields_present_in_model(self):
        """Test that all fields from GSMMetadataDataclass are present in model."""
        # Get all field names from the dataclass
        dataclass_fields = set(GSMMetadataDataclass.__annotations__.keys())

        # Get all column names from the GlobalSkyModelMetadata model
        inspector = inspect(engine)
        Base.metadata.create_all(bind=engine)  # pylint: disable=no-member

        model_columns = set()
        for table_name in inspector.get_table_names():
            if "global_sky_model_metadata" in table_name.lower():
                model_columns = {col["name"] for col in inspector.get_columns(table_name)}
                break

        Base.metadata.drop_all(bind=engine)  # pylint: disable=no-member

        # Check that all dataclass fields are in the model
        missing_fields = dataclass_fields - model_columns
        assert (
            not missing_fields
        ), f"Fields from GSMMetadataDataclass missing in model: {missing_fields}"

    def test_model_has_only_expected_columns(self):
        """Test that GlobalSkyModelMetadata model doesn't have unexpected columns."""
        # Expected columns: dataclass fields + database-specific fields
        expected_columns = set(GSMMetadataDataclass.__annotations__.keys())
        expected_columns.update(
            {
                "id",
                "catalogue_name",
                "description",
                "upload_id",
                "author",
                "reference",
                "notes",
                "uploaded_at",
                "metadata",
                "registry",
            }
        )

        # Get actual model columns
        inspector = inspect(engine)
        Base.metadata.create_all(bind=engine)  # pylint: disable=no-member

        actual_columns = set()
        for table_name in inspector.get_table_names():
            if "global_sky_model_metadata" in table_name.lower():
                actual_columns = {col["name"] for col in inspector.get_columns(table_name)}
                break

        Base.metadata.drop_all(bind=engine)  # pylint: disable=no-member

        unexpected_columns = actual_columns - expected_columns
        assert (
            not unexpected_columns
        ), f"Unexpected columns in GlobalSkyModelMetadata model: {unexpected_columns}"

    def test_field_count_matches(self):
        """Test that the number of fields matches expectations."""
        # Expected: all dataclass fields + all database-specific fields
        expected_count = len(GSMMetadataDataclass.__annotations__) + 8

        # Get actual count
        inspector = inspect(engine)
        Base.metadata.create_all(bind=engine)  # pylint: disable=no-member

        actual_count = 0
        for table_name in inspector.get_table_names():
            if "global_sky_model_metadata" in table_name.lower():
                actual_count = len(inspector.get_columns(table_name))
                break

        Base.metadata.drop_all(bind=engine)  # pylint: disable=no-member

        assert (
            actual_count == expected_count
        ), f"Expected {expected_count} columns, but found {actual_count}"
