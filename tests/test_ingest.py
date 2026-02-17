# pylint: disable=no-member,redefined-outer-name,duplicate-code
"""
Tests for catalogue ingestion functionality
"""

import logging
import tempfile
from pathlib import Path

import pytest
from sqlalchemy import JSON, create_engine, event
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from ska_sdp_global_sky_model.api.app.ingest import (
    ComponentFile,
    build_component_mapping,
    commit_batch,
    compute_hpx_healpy,
    ingest_catalogue,
    parse_catalogue_components,
    process_component_data_batch,
    to_float,
    validate_component_mapping,
)
from ska_sdp_global_sky_model.api.app.models import SkyComponent, SkyComponentStaging
from ska_sdp_global_sky_model.configuration.config import Base

# Test database setup
SQLALCHEMY_DATABASE_URL = "sqlite:///:memory:"
engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


@event.listens_for(Base.metadata, "before_create")
def replace_schema_sqlite(target, connection, **kw):  # pylint: disable=unused-argument
    """Remove schema for SQLite and replace JSONB with JSON."""
    if connection.dialect.name == "sqlite":
        for table in target.tables.values():
            table.schema = None
            for column in table.columns:
                if isinstance(column.type, JSONB):
                    column.type = JSON()


@pytest.fixture()
def test_db():
    """Create test database"""
    Base.metadata.create_all(bind=engine)
    db = TestingSessionLocal()
    yield db
    db.close()
    Base.metadata.drop_all(bind=engine)


@pytest.fixture()
def sample_csv_file():
    """Create a sample CSV file for testing"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write("component_id,ra,dec,i_pol,major_ax,minor_ax,pos_ang,spec_idx,log_spec_idx\n")
        f.write("J001122-334455,10.5,45.2,1.5,0.01,0.008,45.0,-0.7,false\n")
        f.write("J112233-445566,20.3,30.1,2.3,0.02,0.015,90.0,-0.8,true\n")
        f.write("J223344-556677,30.1,-20.5,0.8,,,,-0.5,false\n")
        temp_path = Path(f.name)

    yield temp_path
    temp_path.unlink()


class TestToFloat:
    """Tests for to_float function"""

    def test_valid_float(self):
        """Test conversion of valid float"""
        assert to_float("3.14") == 3.14
        assert to_float(42) == 42.0

    def test_invalid_values(self):
        """Test conversion of invalid values returns None"""
        assert to_float("invalid") is None
        assert to_float(None) is None
        assert to_float("") is None


class TestComputeHpxHealpy:
    """Tests for compute_hpx_healpy function"""

    def test_valid_coordinates(self):
        """Test healpix computation with valid coordinates"""
        hpx = compute_hpx_healpy(45.0, 30.0)
        assert isinstance(hpx, int)
        assert hpx >= 0

    def test_zero_coordinates(self):
        """Test healpix computation at origin"""
        hpx = compute_hpx_healpy(0.0, 0.0)
        assert isinstance(hpx, int)

    def test_negative_dec(self):
        """Test healpix computation with negative declination"""
        hpx = compute_hpx_healpy(180.0, -45.0)
        assert isinstance(hpx, int)
        assert hpx >= 0


class TestComponentFile:
    """Tests for ComponentFile class"""

    def test_component_file_init(self, sample_csv_file):
        """Test ComponentFile initialization with in-memory content"""
        with open(sample_csv_file, "r", encoding="utf-8") as f:
            content = f.read()
        cf = ComponentFile(content)
        assert cf.len == 4  # Header + 3 data rows

    def test_component_file_iteration(self, sample_csv_file):
        """Test iterating through ComponentFile with in-memory content"""
        with open(sample_csv_file, "r", encoding="utf-8") as f:
            content = f.read()
        cf = ComponentFile(content)
        rows = list(cf)
        assert len(rows) == 3
        assert rows[0]["component_id"] == "J001122-334455"
        assert rows[0]["ra"] == "10.5"

    def test_component_file_invalid_content(self):
        """Test ComponentFile with invalid CSV format"""
        # CSV reader will handle empty or invalid CSV format gracefully
        invalid_csv = "Just some random text without CSV structure"
        cf = ComponentFile(invalid_csv)
        # File can be created, but iterating won't produce valid rows
        rows = list(cf)
        # No valid CSV rows are extracted
        assert len(rows) <= 1  # Might have header row with wrong structure

    def test_component_file_length(self, sample_csv_file):
        """Test __len__ method with in-memory content"""
        with open(sample_csv_file, "r", encoding="utf-8") as f:
            content = f.read()
        cf = ComponentFile(content)
        assert len(cf) == 4


class TestBuildComponentMapping:
    """Tests for build_component_mapping function"""

    def test_minimal_mapping(self):
        """Test building mapping with minimal required fields"""
        component_dict = {
            "component_id": "J001122-334455",
            "ra": "10.5",
            "dec": "45.2",
            "i_pol": "1.5",
        }

        mapping = build_component_mapping(component_dict)

        assert mapping["component_id"] == "J001122-334455"
        assert mapping["ra"] == 10.5
        assert mapping["dec"] == 45.2
        assert mapping["i_pol"] == 1.5
        assert "healpix_index" in mapping

    def test_mapping_with_shape_params(self):
        """Test mapping with source shape parameters"""
        component_dict = {
            "component_id": "J001122-334455",
            "ra": "10.5",
            "dec": "45.2",
            "i_pol": "1.5",
            "major_ax": "0.01",
            "minor_ax": "0.008",
            "pos_ang": "45.0",
        }

        mapping = build_component_mapping(component_dict)

        assert mapping["major_ax"] == 0.01
        assert mapping["minor_ax"] == 0.008
        assert mapping["pos_ang"] == 45.0

    def test_mapping_with_spectral_index(self):
        """Test mapping with spectral index"""
        component_dict = {
            "component_id": "J001122-334455",
            "ra": "10.5",
            "dec": "45.2",
            "i_pol": "1.5",
            "spec_idx": "-0.7",
        }

        mapping = build_component_mapping(component_dict)

        assert mapping["spec_idx"] == [-0.7, None, None, None, None]

    def test_mapping_with_spec_idx_invalid_string(self):
        """Test spec_idx with invalid string converts to None"""
        component_dict = {
            "component_id": "J001122-334455",
            "ra": "10.5",
            "dec": "45.2",
            "i_pol": "1.5",
            "spec_idx": "invalid",
        }

        mapping = build_component_mapping(component_dict)

        # Invalid string should create [None, None, None, None, None]
        assert mapping["spec_idx"] == [None, None, None, None, None]

    def test_mapping_with_spec_idx_invalid_type(self):
        """Test spec_idx with invalid type (dict) creates None array"""
        component_dict = {
            "component_id": "J001122-334455",
            "ra": "10.5",
            "dec": "45.2",
            "i_pol": "1.5",
            "spec_idx": {"invalid": "type"},
        }

        mapping = build_component_mapping(component_dict)

        # Invalid type should create [None, None, None, None, None]
        assert mapping["spec_idx"] == [None, None, None, None, None]

    def test_mapping_with_polarization(self):
        """Test mapping with polarization parameters"""
        component_dict = {
            "component_id": "J001122-334455",
            "ra": "10.5",
            "dec": "45.2",
            "i_pol": "1.5",
            "q_pol": "0.1",
            "u_pol": "0.2",
            "v_pol": "0.05",
        }

        mapping = build_component_mapping(component_dict)

        assert mapping["q_pol"] == 0.1
        assert mapping["u_pol"] == 0.2
        assert mapping["v_pol"] == 0.05

    def test_mapping_with_log_spec_idx(self):
        """Test log_spec_idx boolean conversion"""
        component_dict = {
            "component_id": "J001122-334455",
            "ra": "10.5",
            "dec": "45.2",
            "i_pol": "1.5",
            "log_spec_idx": "true",
        }

        mapping = build_component_mapping(component_dict)
        assert mapping["log_spec_idx"] is True

        component_dict["log_spec_idx"] = "false"
        mapping = build_component_mapping(component_dict)
        assert mapping["log_spec_idx"] is False


class TestValidateComponentMapping:
    """Tests for validate_component_mapping function"""

    def test_valid_minimal_mapping(self):
        """Test validation of minimal valid mapping"""
        mapping = {
            "component_id": "J001122-334455",
            "ra": 10.5,
            "dec": 45.2,
            "i_pol": 1.5,
        }
        is_valid, error = validate_component_mapping(mapping)
        assert is_valid is True
        assert error is None

    def test_missing_required_field(self):
        """Test validation fails for missing required field"""
        mapping = {
            "component_id": "J001122-334455",
            "ra": 10.5,
            # Missing dec
            "i_pol": 1.5,
        }
        is_valid, error = validate_component_mapping(mapping)
        assert is_valid is False
        assert "dec" in error

    def test_invalid_ra_range(self):
        """Test validation fails for RA out of range"""
        mapping = {
            "component_id": "J001122-334455",
            "ra": 400.0,  # Invalid
            "dec": 45.2,
            "i_pol": 1.5,
        }
        is_valid, error = validate_component_mapping(mapping)
        assert is_valid is False
        assert "ra" in error and "out of range" in error

    def test_invalid_dec_range(self):
        """Test validation fails for DEC out of range"""
        mapping = {
            "component_id": "J001122-334455",
            "ra": 10.5,
            "dec": 95.0,  # Invalid
            "i_pol": 1.5,
        }
        is_valid, error = validate_component_mapping(mapping)
        assert is_valid is False
        assert "dec" in error and "out of range" in error

    def test_invalid_type(self):
        """Test validation fails for wrong type"""
        mapping = {
            "component_id": "J001122-334455",
            "ra": "not_a_number",  # Invalid type
            "dec": 45.2,
            "i_pol": 1.5,
        }
        is_valid, error = validate_component_mapping(mapping)
        assert is_valid is False
        assert "type" in error.lower()


class TestCommitBatch:
    """Tests for commit_batch function"""

    def test_commit_empty_batch(self, test_db):
        """Test committing empty batch does nothing"""
        component_objs = []
        commit_batch(test_db, component_objs)
        count = test_db.query(SkyComponent).count()
        assert count == 0

    def test_commit_batch_with_components(self, test_db):
        """Test committing batch with components"""
        component_objs = [
            {
                "component_id": "J001122-334455",
                "healpix_index": 12345,
                "ra": 10.5,
                "dec": 45.2,
                "i_pol": 1.5,
                "version": "0.1.0",
            },
            {
                "component_id": "J223344-556677",
                "healpix_index": 67890,
                "ra": 30.1,
                "dec": -20.5,
                "i_pol": 0.8,
                "version": "0.1.0",
            },
        ]
        commit_batch(test_db, component_objs)

        count = test_db.query(SkyComponent).count()
        assert count == 2
        assert len(component_objs) == 0  # Should be cleared


class TestParseCatalogueComponents:  # pylint: disable=too-few-public-methods
    """Tests for parse_catalogue_components function"""

    def test_content_based_selector(self, sample_csv_file):
        """Test content-based catalogue parsing for in-memory CSV data"""
        with open(sample_csv_file, "r", encoding="utf-8") as f:
            content = f.read()

        ingest_metadata = {
            "file_location": [
                {
                    "content": content,
                }
            ]
        }

        results = list(parse_catalogue_components(ingest_metadata))
        assert len(results) == 1
        component_file = results[0]
        assert isinstance(component_file, ComponentFile)


class TestProcessComponentDataBatch:
    """Tests for process_component_data_batch function"""

    def test_process_valid_components(self, test_db, sample_csv_file):
        """Test processing valid component data from in-memory content"""
        with open(sample_csv_file, "r", encoding="utf-8") as f:
            content = f.read()
        cf = ComponentFile(content)

        result = process_component_data_batch(test_db, cf, staging=True, upload_id="test-upload-1")

        assert result is True
        count = test_db.query(SkyComponentStaging).count()
        assert count == 3

    def test_skip_duplicate_component_id(self, test_db, sample_csv_file):
        """Test that duplicate component IDs are allowed across different uploads"""
        with open(sample_csv_file, "r", encoding="utf-8") as f:
            content = f.read()

        cf = ComponentFile(content)

        # First ingestion with upload_id 1
        process_component_data_batch(test_db, cf, staging=True, upload_id="test-upload-1")
        count1 = test_db.query(SkyComponentStaging).count()

        # Second ingestion with different upload_id - should allow duplicates
        cf2 = ComponentFile(content)
        process_component_data_batch(test_db, cf2, staging=True, upload_id="test-upload-2")
        count2 = test_db.query(SkyComponentStaging).count()

        # Both uploads should succeed with same component_ids
        assert count2 == count1 * 2  # Double the components (different upload_ids)

    def test_validation_errors_prevent_ingestion(self, test_db):
        """Test that validation errors prevent any data from being ingested (all-or-nothing)"""
        # Create CSV with mix of valid and invalid components
        invalid_csv = (
            "component_id,ra,dec,i_pol\n"
            "J001122-334455,10.5,45.2,1.5\n"  # Valid
            "J112233-445566,400.0,30.1,2.3\n"  # Invalid RA (out of range)
            "J223344-556677,30.1,-20.5,0.8\n"  # Valid
        )

        cf = ComponentFile(invalid_csv)

        result = process_component_data_batch(test_db, cf, staging=True, upload_id="test-upload-1")

        # Should fail due to validation errors
        assert result is False
        # NO data should be ingested (all-or-nothing)
        count = test_db.query(SkyComponentStaging).count()
        assert count == 0

    def test_all_validation_errors_collected(self, test_db, caplog):
        """Test that all validation errors are collected and logged"""
        caplog.set_level(logging.ERROR)

        # Create CSV with multiple invalid components
        invalid_csv = (
            "component_id,ra,dec,i_pol\n"
            "J001122-334455,10.5,45.2,1.5\n"  # Valid
            "J112233-445566,400.0,30.1,2.3\n"  # Invalid RA
            "J223344-556677,30.1,95.0,0.8\n"  # Invalid DEC
            "J334455-667788,20.0,10.0,-1.0\n"  # Valid (i_pol no longer validated)
            "J445566-778899,50.0,20.0,2.0\n"  # Valid
        )

        cf = ComponentFile(invalid_csv)

        result = process_component_data_batch(test_db, cf)

        # Should fail
        assert result is False

        # Check that all 2 validation errors were logged (only RA and DEC are validated)
        error_logs = [record.message for record in caplog.records if record.levelname == "ERROR"]

        # Should have summary + all errors logged
        validation_summary = [log for log in error_logs if "Validation failed with" in log]
        assert len(validation_summary) == 1
        assert "2 errors" in validation_summary[0]

        # All errors should be logged (not just first 10)
        all_errors_log = [log for log in error_logs if "All validation errors:" in log]
        assert len(all_errors_log) == 1

        # Check individual error messages are present
        error_messages = [log for log in error_logs if "Row" in log and "component_id:" in log]
        assert len(error_messages) == 2  # Only RA and DEC validation errors

    def test_validation_phase_before_ingestion(self, test_db, caplog):
        """Test that all validation happens before any ingestion"""
        caplog.set_level(logging.INFO)

        # Create CSV with invalid data at the end
        invalid_csv = (
            "component_id,ra,dec,i_pol\n"
            "J001122-334455,10.5,45.2,1.5\n"  # Valid
            "J112233-445566,20.0,30.1,2.3\n"  # Valid
            "J223344-556677,400.0,-20.5,0.8\n"  # Invalid RA (last row)
        )

        cf = ComponentFile(invalid_csv)

        result = process_component_data_batch(test_db, cf, staging=True, upload_id="test-upload-1")

        # Should fail
        assert result is False

        # NO data should be ingested, even though first 2 rows were valid
        count = test_db.query(SkyComponentStaging).count()
        assert count == 0

        # Check that validation message appeared before any ingestion message
        log_messages = [record.message for record in caplog.records]
        validation_msg_found = False
        ingestion_msg_found = False

        for msg in log_messages:
            if "Validating all component data" in msg:
                validation_msg_found = True
            if "Starting ingestion" in msg:
                ingestion_msg_found = True

        assert validation_msg_found is True
        # Should NOT have ingestion message since validation failed
        assert ingestion_msg_found is False

    def test_successful_ingestion_after_validation(self, test_db, caplog):
        """Test that ingestion proceeds only after all validation passes"""
        caplog.set_level(logging.INFO)

        valid_csv = (
            "component_id,ra,dec,i_pol\n"
            "J001122-334455,10.5,45.2,1.5\n"
            "J112233-445566,20.0,30.1,2.3\n"
        )

        cf = ComponentFile(valid_csv)

        result = process_component_data_batch(test_db, cf, staging=True, upload_id="test-upload-1")

        # Should succeed
        assert result is True
        count = test_db.query(SkyComponentStaging).count()
        assert count == 2

        # Check log sequence
        log_messages = [record.message for record in caplog.records]

        # Find validation and ingestion messages
        validation_complete = False
        ingestion_started = False

        for msg in log_messages:
            if "validated successfully" in msg:
                validation_complete = True
            if "Starting ingestion" in msg and validation_complete:
                ingestion_started = True

        assert validation_complete is True
        assert ingestion_started is True


class TestIngestCatalogue:
    """Tests for ingest_catalogue function"""

    def test_successful_ingestion(self, test_db, sample_csv_file):
        """Test successful full catalogue ingestion with in-memory content"""
        with open(sample_csv_file, "r", encoding="utf-8") as f:
            content = f.read()

        catalogue_metadata = {
            "name": "Test Catalogue",
            "catalogue_name": "TEST",
            "staging": True,
            "upload_id": "test-upload-1",
            "ingest": {
                "file_location": [
                    {
                        "content": content,
                    }
                ]
            },
        }

        result = ingest_catalogue(test_db, catalogue_metadata)

        assert result is True
        count = test_db.query(SkyComponentStaging).count()
        assert count == 3

    def test_empty_catalogue(self, test_db):
        """Test handling of empty catalogue with in-memory content"""
        # Create empty CSV content
        empty_content = "component_id,ra,dec,i_pol\n"

        catalogue_metadata = {
            "name": "Empty Catalogue",
            "catalogue_name": "EMPTY",
            "ingest": {
                "file_location": [
                    {
                        "content": empty_content,
                    }
                ]
            },
        }

        result = ingest_catalogue(test_db, catalogue_metadata)
        # Should succeed with empty catalogue
        assert result is True
