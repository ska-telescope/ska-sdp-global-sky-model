"""
Unit tests for the database models.

These tests validate that the SQLAlchemy models work correctly,
including their methods and field configurations. Both SkyComponent and
GlobalSkyModelMetadata models: dynamically generated columns from dataclasses
with hardcoded database-specific fields and methods.

The engine is used in conftest.py to generate the database with the right tables,
results of which are tested here.
"""

import pytest
from ska_sdp_datamodels.global_sky_model.global_sky_model import (
    GlobalSkyModelMetadata as GSMMetadataDataclass,
)
from ska_sdp_datamodels.global_sky_model.global_sky_model import (
    SkyComponent,
)
from sqlalchemy import event, inspect
from sqlalchemy.exc import IntegrityError

from ska_sdp_global_sky_model.api.app.models import GlobalSkyModelMetadata
from ska_sdp_global_sky_model.api.app.models import SkyComponent as SkyComponentModel
from ska_sdp_global_sky_model.api.app.models import SkyComponentStaging
from tests.utils import clean_all_tables, engine, override_get_db


@pytest.fixture(scope="function", autouse=True)
def clean_up_database():
    """
    Clean tables after each test run.
    Specific to this module. Do not move.
    """
    yield
    clean_all_tables()


# pylint: disable=unused-argument
def q3c_radial_query_mock(*args):
    """Mock q3c_radial_query function."""
    return True


@event.listens_for(engine, "connect")
def register_sqlite_functions(dbapi_connection, connection_record):
    """Load the mock function for q3c_radial_query"""
    dbapi_connection.create_function("q3c_radial_query", 5, q3c_radial_query_mock)


class TestSkyComponentModel:
    """Tests for the SkyComponent SQLAlchemy model."""

    def test_sky_component_table_exists(self):
        """Test that SkyComponent table can be created."""
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        assert "sky_component" in tables or any("sky_component" in t.lower() for t in tables)

    def test_sky_component_has_required_columns(self):
        """Test that SkyComponent model has all required columns."""
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
        assert "ra_deg" in columns
        assert "dec_deg" in columns
        assert "i_pol_jy" in columns

    def test_sky_component_with_optional_fields(self, gsm_metadata):
        """Test creating a SkyComponent with optional fields."""
        db_session = next(override_get_db())
        db_session.add(gsm_metadata)
        db_session.commit()
        component = SkyComponentModel(
            component_id="TestSource2",
            ra_deg=45.67,
            dec_deg=12.34,
            i_pol_jy=5.67,
            a_arcsec=0.001,
            b_arcsec=0.0005,
            pa_deg=1.57,
            spec_idx=[1.0, -0.5, 0.1],
            log_spec_idx=True,
            gsm_id=gsm_metadata.id,
        )

        db_session.add(component)
        db_session.commit()

        retrieved = (
            db_session.query(SkyComponentModel).filter_by(component_id="TestSource2").first()
        )
        assert retrieved.component_id == "TestSource2"
        assert retrieved.ra_deg == 45.67
        assert retrieved.dec_deg == 12.34
        assert retrieved.i_pol_jy == 5.67
        # optional fields below
        assert retrieved.a_arcsec == 0.001
        assert retrieved.b_arcsec == 0.0005
        assert retrieved.pa_deg == 1.57
        assert retrieved.spec_idx == [1.0, -0.5, 0.1]
        assert retrieved.log_spec_idx is True

    def test_sky_component_versioning_constraint(self, gsm_metadata):
        """Test that duplicate component_id + version raises constraint error."""
        db_session = next(override_get_db())
        db_session.add(gsm_metadata)
        db_session.commit()
        component1 = SkyComponentModel(
            component_id="VersionedSource",
            ra_deg=100.0,
            dec_deg=50.0,
            i_pol_jy=1.0,
            gsm_id=gsm_metadata.id,
        )
        db_session.add(component1)
        db_session.commit()

        # Try to create duplicate component_id + version
        component2 = SkyComponentModel(
            component_id="VersionedSource",
            ra_deg=200.0,
            dec_deg=60.0,
            i_pol_jy=2.0,
            gsm_id=gsm_metadata.id,
        )
        db_session.add(component2)

        with pytest.raises(Exception):  # Should raise IntegrityError
            db_session.commit()

    def test_sky_component_columns_to_dict_method(self):
        """Test the columns_to_dict method."""
        component = SkyComponentModel(
            component_id="DictTestSource",
            ra_deg=111.11,
            dec_deg=-22.22,
            i_pol_jy=3.33,
            a_arcsec=0.002,
            gsm_id=1,
        )

        # Test columns_to_dict
        component_dict = component.columns_to_dict()

        assert isinstance(component_dict, dict)
        assert component_dict["component_id"] == "DictTestSource"
        assert component_dict["ra_deg"] == 111.11
        assert component_dict["dec_deg"] == -22.22
        assert component_dict["i_pol_jy"] == 3.33
        assert component_dict["a_arcsec"] == 0.002
        assert "id" in component_dict

    def test_sky_component_nullable_fields(self, gsm_metadata):
        """Test that nullable fields can be None."""
        db_session = next(override_get_db())
        db_session.add(gsm_metadata)
        db_session.commit()
        component = SkyComponentModel(
            component_id="NullTestSource",
            ra_deg=180.0,
            dec_deg=0.0,
            i_pol_jy=1.0,
            gsm_id=gsm_metadata.id,
            # All optional fields left as None
        )
        db_session.add(component)
        db_session.commit()

        retrieved = (
            db_session.query(SkyComponentModel).filter_by(component_id="NullTestSource").first()
        )
        assert retrieved.a_arcsec is None
        assert retrieved.b_arcsec is None
        assert retrieved.pa_deg is None
        assert retrieved.spec_idx is None
        assert retrieved.log_spec_idx is None

    def test_sky_component_spec_idx_as_json(self, gsm_metadata):
        """Test that spec_idx field properly stores JSON data.
        TODO: how is this JSON? Is this a leftover from some previous version of specidx?
        """
        spec_idx_values = [1.5, -0.7, 0.2, -0.05, 0.01]
        db_session = next(override_get_db())
        db_session.add(gsm_metadata)
        db_session.commit()
        component = SkyComponentModel(
            component_id="SpecIdxSource",
            ra_deg=90.0,
            dec_deg=45.0,
            i_pol_jy=2.0,
            spec_idx=spec_idx_values,
            gsm_id=gsm_metadata.id,
        )
        db_session.add(component)
        db_session.commit()

        retrieved = (
            db_session.query(SkyComponentModel).filter_by(component_id="SpecIdxSource").first()
        )
        assert retrieved.spec_idx == spec_idx_values
        assert isinstance(retrieved.spec_idx, list)
        assert len(retrieved.spec_idx) == 5

    def test_sky_component_versioning(self, gsm_metadata):
        """Test versioning support, same component_id can have multiple versions."""
        db_session = next(override_get_db())
        db_session.add(gsm_metadata)
        db_session.commit()
        second_metadata = GlobalSkyModelMetadata(
            version="2.0.0",
            catalogue_name="EMPTY",
            upload_id="test-upload-2",
        )
        db_session.add(second_metadata)
        db_session.commit()
        # Create two versions of the same component
        component_v1 = SkyComponentModel(
            component_id="VersionedSource",
            ra_deg=100.0,
            dec_deg=50.0,
            i_pol_jy=1.0,
            gsm_id=gsm_metadata.id,
        )
        component_v2 = SkyComponentModel(
            component_id="VersionedSource",
            ra_deg=100.1,
            dec_deg=50.1,
            i_pol_jy=1.1,
            gsm_id=second_metadata.id,
        )
        db_session.add_all([component_v1, component_v2])
        db_session.commit()

        # Both versions should exist
        all_versions = (
            db_session.query(SkyComponentModel).filter_by(component_id="VersionedSource").all()
        )
        assert len(all_versions) == 2
        assert {v.gsm_id for v in all_versions} == {gsm_metadata.id, second_metadata.id}


class TestSkyComponentStagingModel:
    """Tests for the SkyComponentStaging SQLAlchemy model."""

    def test_staging_basic_functionality(self, gsm_metadata):
        """Test basic SkyComponentStaging CRUD operations."""
        db_session = next(override_get_db())
        db_session.add(gsm_metadata)
        db_session.commit()
        staged = SkyComponentStaging(
            component_id="StagedSource1",
            upload_id="test-upload-123",
            ra_deg=123.45,
            dec_deg=-67.89,
            i_pol_jy=1.23,
            gsm_id=gsm_metadata.id,
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

    def test_staging_allows_same_component_different_uploads(self, gsm_metadata):
        """Test that same component_id can exist in different uploads."""
        db_session = next(override_get_db())
        db_session.add(gsm_metadata)
        db_session.commit()
        staged1 = SkyComponentStaging(
            component_id="TestSource",
            upload_id="upload-001",
            ra_deg=100.0,
            dec_deg=50.0,
            i_pol_jy=1.0,
            gsm_id=gsm_metadata.id,
        )
        db_session.add(staged1)
        db_session.commit()

        # Same component_id in different upload - should succeed
        staged2 = SkyComponentStaging(
            component_id="TestSource",
            upload_id="upload-002",
            ra_deg=200.0,
            dec_deg=60.0,
            i_pol_jy=2.0,
            gsm_id=gsm_metadata.id,
        )
        db_session.add(staged2)
        db_session.commit()

        # Both should exist
        all_records = (
            db_session.query(SkyComponentStaging).filter_by(component_id="TestSource").all()
        )
        assert len(all_records) == 2

    def test_staging_unique_constraint_violation(self, gsm_metadata):
        """Test that duplicate component_id + upload_id raises constraint error."""
        db_session = next(override_get_db())
        db_session.add(gsm_metadata)
        db_session.commit()
        staged1 = SkyComponentStaging(
            component_id="TestSource",
            upload_id="upload-001",
            ra_deg=100.0,
            dec_deg=50.0,
            i_pol_jy=1.0,
            gsm_id=gsm_metadata.id,
        )
        db_session.add(staged1)
        db_session.commit()

        # Duplicate component_id + upload_id - should fail
        staged2 = SkyComponentStaging(
            component_id="TestSource",
            upload_id="upload-001",
            ra_deg=200.0,
            dec_deg=60.0,
            i_pol_jy=2.0,
            gsm_id=gsm_metadata.id,
        )
        db_session.add(staged2)
        with pytest.raises(IntegrityError):
            db_session.commit()


class TestGlobalSkyModelMetadataModel:
    """Tests for the GlobalSkyModelMetadata SQLAlchemy model."""

    def test_metadata_table_exists(self):
        """Test that GlobalSkyModelMetadata table can be created."""
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        assert "global_sky_model_metadata" in tables or any(
            "global_sky_model_metadata" in t.lower() for t in tables
        )

    def test_metadata_has_required_columns(self):
        """Test that GlobalSkyModelMetadata model has all required columns."""
        inspector = inspect(engine)

        columns = []
        for table_name in inspector.get_table_names():
            if "global_sky_model_metadata" in table_name.lower():
                columns = [col["name"] for col in inspector.get_columns(table_name)]
                break

        assert "id" in columns
        assert "version" in columns
        assert "catalogue_name" in columns

    def test_metadata_create_instance(self):
        """Test creating a GlobalSkyModelMetadata instance."""

        metadata = GlobalSkyModelMetadata(
            version="1.0.0",
            catalogue_name="TestCatalogue",
            upload_id="test-upload-1",
        )
        db_session = next(override_get_db())
        db_session.add(metadata)
        db_session.commit()

        retrieved = db_session.query(GlobalSkyModelMetadata).filter_by(version="1.0.0").first()
        assert retrieved is not None
        assert retrieved.version == "1.0.0"

    def test_metadata_columns_to_dict_method(self):
        """Test the columns_to_dict method for GlobalSkyModelMetadata."""
        metadata = GlobalSkyModelMetadata(
            version="2.1.0",
            catalogue_name="TestCatalogue",
            upload_id="test-upload-2",
        )

        metadata_dict = metadata.columns_to_dict()

        assert isinstance(metadata_dict, dict)
        assert metadata_dict["version"] == "2.1.0"
        assert metadata_dict["catalogue_name"] == "TestCatalogue"
        assert "id" in metadata_dict

    def test_metadata_multiple_versions(self):
        """Test storing multiple metadata versions."""
        metadata1 = GlobalSkyModelMetadata(
            version="1.0.0",
            catalogue_name="TestCatalogue1",
            upload_id="test-upload-1",
        )
        metadata2 = GlobalSkyModelMetadata(
            version="2.0.0",
            catalogue_name="TestCatalogue2",
            upload_id="test-upload-2",
        )
        metadata3 = GlobalSkyModelMetadata(
            version="3.0.0",
            catalogue_name="TestCatalogue3",
            upload_id="test-upload-3",
        )

        db_session = next(override_get_db())
        db_session.add_all([metadata1, metadata2, metadata3])
        db_session.commit()

        # Query all metadata entries
        all_metadata = db_session.query(GlobalSkyModelMetadata).all()
        assert len(all_metadata) == 3

        versions = {m.version for m in all_metadata}
        assert versions == {"1.0.0", "2.0.0", "3.0.0"}


class TestModelIntegration:  # pylint: disable=too-few-public-methods
    """Integration tests for models working together."""

    def test_sky_component_and_metadata_coexist(self):
        """Test that SkyComponent and Metadata can both be stored in the same database."""
        # Create metadata
        metadata = GlobalSkyModelMetadata(
            version="1.0.0",
            catalogue_name="TestCatalogue",
            upload_id="test-upload-1",
        )
        db_session = next(override_get_db())
        db_session.add(metadata)

        # Create components
        component1 = SkyComponentModel(
            component_id="IntegrationSource1",
            ra_deg=100.0,
            dec_deg=50.0,
            i_pol_jy=1.5,
            gsm_id=metadata.id,
            ref_freq_hz=1.4e9,
        )
        component2 = SkyComponentModel(
            component_id="IntegrationSource2",
            ra_deg=200.0,
            dec_deg=-30.0,
            i_pol_jy=2.5,
            gsm_id=metadata.id,
            ref_freq_hz=1.4e9,
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

        model_columns = set()
        for table_name in inspector.get_table_names():
            if "sky_component" in table_name.lower():
                model_columns = {col["name"] for col in inspector.get_columns(table_name)}
                break

        # Check that all dataclass fields are in the model
        # (excluding 'id' which is database-specific)
        missing_fields = dataclass_fields - model_columns
        assert (
            not missing_fields
        ), f"Fields from SkyComponent dataclass missing in SkyComponentModel: {missing_fields}"

    def test_model_has_only_expected_columns(self):
        """Test that SkyComponentModel doesn't have unexpected columns."""
        # Expected columns: dataclass fields + database-specific fields
        expected_columns = set(SkyComponent.__annotations__.keys())
        expected_columns.update(["id", "gsm_id"])

        # Get actual model columns
        inspector = inspect(engine)

        actual_columns = set()
        for table_name in inspector.get_table_names():
            if "sky_component" in table_name.lower():
                actual_columns = {col["name"] for col in inspector.get_columns(table_name)}
                break

        unexpected_columns = actual_columns - expected_columns
        assert (
            not unexpected_columns
        ), f"Unexpected columns in SkyComponentModel: {unexpected_columns}"

    def test_field_count_matches(self):
        """Test that the number of fields matches expectations."""
        # Expected: all dataclass fields + 2 database-specific
        expected_count = len(SkyComponent.__annotations__) + 2

        # Get actual count
        inspector = inspect(engine)

        actual_count = 0
        for table_name in inspector.get_table_names():
            if "sky_component" in table_name.lower():
                actual_count = len(inspector.get_columns(table_name))
                break

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

        model_columns = set()
        for table_name in inspector.get_table_names():
            if "global_sky_model_metadata" in table_name.lower():
                model_columns = {col["name"] for col in inspector.get_columns(table_name)}
                break

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
                "staging",
            }
        )

        # Get actual model columns
        inspector = inspect(engine)

        actual_columns = set()
        for table_name in inspector.get_table_names():
            if "global_sky_model_metadata" in table_name.lower():
                actual_columns = {col["name"] for col in inspector.get_columns(table_name)}
                break

        unexpected_columns = actual_columns - expected_columns
        assert (
            not unexpected_columns
        ), f"Unexpected columns in GlobalSkyModelMetadata model: {unexpected_columns}"

    def test_field_count_matches(self):
        """Test that the number of fields matches expectations."""
        # Expected: all dataclass fields + all database-specific fields
        expected_count = len(GSMMetadataDataclass.__annotations__) + 3

        # Get actual count
        inspector = inspect(engine)

        actual_count = 0
        for table_name in inspector.get_table_names():
            if "global_sky_model_metadata" in table_name.lower():
                actual_count = len(inspector.get_columns(table_name))
                break

        assert (
            actual_count == expected_count
        ), f"Expected {expected_count} columns, but found {actual_count}"
