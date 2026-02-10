# pylint: disable=no-member,redefined-outer-name,duplicate-code
"""
Tests for catalog ingestion functionality
"""

import tempfile
from pathlib import Path

import pytest
from sqlalchemy import JSON, create_engine, event
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from ska_sdp_global_sky_model.api.app.ingest import (
    SourceFile,
    build_source_mapping,
    coerce_floats,
    commit_batch,
    compute_hpx_healpy,
    get_data_catalog_selector,
    get_full_catalog,
    process_source_data_batch,
    to_float,
    validate_source_mapping,
)
from ska_sdp_global_sky_model.api.app.models import SkyComponent
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


class TestCoerceFloats:
    """Tests for coerce_floats function"""

    def test_coerce_valid_floats(self):
        """Test coercion of valid numeric strings"""
        input_dict = {"a": "1.5", "b": "2", "c": 3.14}
        result = coerce_floats(input_dict)
        assert result["a"] == 1.5
        assert result["b"] == 2.0
        assert result["c"] == 3.14

    def test_coerce_invalid_values(self):
        """Test that invalid values remain unchanged"""
        input_dict = {"a": "text", "b": None, "c": ""}
        result = coerce_floats(input_dict)
        assert result["a"] == "text"
        assert result["b"] is None
        assert result["c"] == ""


class TestSourceFile:
    """Tests for SourceFile class"""

    def test_source_file_init(self, sample_csv_file):
        """Test SourceFile initialization with in-memory content"""
        with open(sample_csv_file, "rb") as f:
            content = f.read()
        sf = SourceFile(content)
        assert sf.len == 4  # Header + 3 data rows

    def test_source_file_iteration(self, sample_csv_file):
        """Test iterating through SourceFile with in-memory content"""
        with open(sample_csv_file, "rb") as f:
            content = f.read()
        sf = SourceFile(content)
        rows = list(sf)
        assert len(rows) == 3
        assert rows[0]["component_id"] == "J001122-334455"
        assert rows[0]["ra"] == "10.5"

    def test_source_file_invalid_content(self):
        """Test SourceFile with invalid content"""
        with pytest.raises(Exception):  # Will raise UnicodeDecodeError or similar
            SourceFile(b"\x80\x81\x82")  # Invalid UTF-8

    def test_source_file_length(self, sample_csv_file):
        """Test __len__ method with in-memory content"""
        with open(sample_csv_file, "rb") as f:
            content = f.read()
        sf = SourceFile(content)
        assert len(sf) == 4


class TestBuildSourceMapping:
    """Tests for build_source_mapping function"""

    def test_minimal_mapping(self):
        """Test building mapping with minimal required fields"""
        source_dict = {
            "component_id": "J001122-334455",
            "ra": "10.5",
            "dec": "45.2",
            "i_pol": "1.5",
        }
        catalog_config = {"source": "component_id"}

        mapping = build_source_mapping(source_dict, catalog_config)

        assert mapping["component_id"] == "J001122-334455"
        assert mapping["ra"] == 10.5
        assert mapping["dec"] == 45.2
        assert mapping["i_pol"] == 1.5
        assert "healpix_index" in mapping

    def test_mapping_with_shape_params(self):
        """Test mapping with source shape parameters"""
        source_dict = {
            "component_id": "J001122-334455",
            "ra": "10.5",
            "dec": "45.2",
            "i_pol": "1.5",
            "major_ax": "0.01",
            "minor_ax": "0.008",
            "pos_ang": "45.0",
        }
        catalog_config = {"source": "component_id"}

        mapping = build_source_mapping(source_dict, catalog_config)

        assert mapping["major_ax"] == 0.01
        assert mapping["minor_ax"] == 0.008
        assert mapping["pos_ang"] == 45.0

    def test_mapping_with_spectral_index(self):
        """Test mapping with spectral index"""
        source_dict = {
            "component_id": "J001122-334455",
            "ra": "10.5",
            "dec": "45.2",
            "i_pol": "1.5",
            "spec_idx": "-0.7",
        }
        catalog_config = {"source": "component_id"}

        mapping = build_source_mapping(source_dict, catalog_config)

        assert mapping["spec_idx"] == [-0.7, None, None, None, None]

    def test_mapping_with_polarization(self):
        """Test mapping with polarization parameters"""
        source_dict = {
            "component_id": "J001122-334455",
            "ra": "10.5",
            "dec": "45.2",
            "i_pol": "1.5",
            "q_pol": "0.1",
            "u_pol": "0.2",
            "v_pol": "0.05",
        }
        catalog_config = {"source": "component_id"}

        mapping = build_source_mapping(source_dict, catalog_config)

        assert mapping["q_pol"] == 0.1
        assert mapping["u_pol"] == 0.2
        assert mapping["v_pol"] == 0.05

    def test_mapping_with_log_spec_idx(self):
        """Test log_spec_idx boolean conversion"""
        source_dict = {
            "component_id": "J001122-334455",
            "ra": "10.5",
            "dec": "45.2",
            "i_pol": "1.5",
            "log_spec_idx": "true",
        }
        catalog_config = {"source": "component_id"}

        mapping = build_source_mapping(source_dict, catalog_config)
        assert mapping["log_spec_idx"] is True

        source_dict["log_spec_idx"] = "false"
        mapping = build_source_mapping(source_dict, catalog_config)
        assert mapping["log_spec_idx"] is False


class TestValidateSourceMapping:
    """Tests for validate_source_mapping function"""

    def test_valid_minimal_mapping(self):
        """Test validation of minimal valid mapping"""
        mapping = {
            "component_id": "J001122-334455",
            "ra": 10.5,
            "dec": 45.2,
            "i_pol": 1.5,
        }
        is_valid, error = validate_source_mapping(mapping)
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
        is_valid, error = validate_source_mapping(mapping)
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
        is_valid, error = validate_source_mapping(mapping)
        assert is_valid is False
        assert "RA" in error

    def test_invalid_dec_range(self):
        """Test validation fails for DEC out of range"""
        mapping = {
            "component_id": "J001122-334455",
            "ra": 10.5,
            "dec": 95.0,  # Invalid
            "i_pol": 1.5,
        }
        is_valid, error = validate_source_mapping(mapping)
        assert is_valid is False
        assert "DEC" in error

    def test_negative_flux(self):
        """Test validation fails for negative flux"""
        mapping = {
            "component_id": "J001122-334455",
            "ra": 10.5,
            "dec": 45.2,
            "i_pol": -1.5,  # Invalid
        }
        is_valid, error = validate_source_mapping(mapping)
        assert is_valid is False
        assert "i_pol" in error

    def test_invalid_type(self):
        """Test validation fails for wrong type"""
        mapping = {
            "component_id": "J001122-334455",
            "ra": "not_a_number",  # Invalid type
            "dec": 45.2,
            "i_pol": 1.5,
        }
        is_valid, error = validate_source_mapping(mapping)
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
            },
            {
                "component_id": "J223344-556677",
                "healpix_index": 67890,
                "ra": 30.1,
                "dec": -20.5,
                "i_pol": 0.8,
            },
        ]
        commit_batch(test_db, component_objs)

        count = test_db.query(SkyComponent).count()
        assert count == 2
        assert len(component_objs) == 0  # Should be cleared


class TestGetDataCatalogSelector:  # pylint: disable=too-few-public-methods
    """Tests for get_data_catalog_selector function"""

    def test_content_based_selector(self, sample_csv_file):
        """Test content-based catalog selector for API bulk upload"""
        with open(sample_csv_file, "rb") as f:
            content = f.read()

        ingest_config = {
            "file_location": [
                {
                    "content": content,
                    "bands": [150],
                }
            ]
        }

        results = list(get_data_catalog_selector(ingest_config))
        assert len(results) == 1
        source_file, bands = results[0]
        assert isinstance(source_file, SourceFile)
        assert bands == [150]


class TestProcessSourceDataBatch:
    """Tests for process_source_data_batch function"""

    def test_process_valid_sources(self, test_db, sample_csv_file):
        """Test processing valid source data from in-memory content"""
        with open(sample_csv_file, "rb") as f:
            content = f.read()
        sf = SourceFile(content)
        catalog_config = {"source": "component_id"}

        result = process_source_data_batch(test_db, sf, catalog_config)

        assert result is True
        count = test_db.query(SkyComponent).count()
        assert count == 3

    def test_skip_duplicate_component_id(self, test_db, sample_csv_file):
        """Test that duplicate component IDs are skipped"""
        with open(sample_csv_file, "rb") as f:
            content = f.read()

        sf = SourceFile(content)
        catalog_config = {"source": "component_id"}

        # First ingestion
        process_source_data_batch(test_db, sf, catalog_config)
        count1 = test_db.query(SkyComponent).count()

        # Second ingestion - should skip duplicates
        sf2 = SourceFile(content)
        process_source_data_batch(test_db, sf2, catalog_config)
        count2 = test_db.query(SkyComponent).count()

        assert count1 == count2  # No new components added


class TestGetFullCatalog:
    """Tests for get_full_catalog function"""

    def test_successful_ingestion(self, test_db, sample_csv_file):
        """Test successful full catalog ingestion with in-memory content"""
        with open(sample_csv_file, "rb") as f:
            content = f.read()

        catalog_config = {
            "name": "Test Catalog",
            "catalog_name": "TEST",
            "source": "component_id",
            "ingest": {
                "file_location": [
                    {
                        "content": content,
                        "bands": [],
                    }
                ]
            },
        }

        result = get_full_catalog(test_db, catalog_config)

        assert result is True
        count = test_db.query(SkyComponent).count()
        assert count == 3

    def test_empty_catalog(self, test_db):
        """Test handling of empty catalog with in-memory content"""
        # Create empty CSV content
        empty_content = b"component_id,ra,dec,i_pol\n"

        catalog_config = {
            "name": "Empty Catalog",
            "catalog_name": "EMPTY",
            "source": "component_id",
            "ingest": {
                "file_location": [
                    {
                        "content": empty_content,
                        "bands": [],
                    }
                ]
            },
        }

        result = get_full_catalog(test_db, catalog_config)
        # Should succeed with empty catalog
        assert result is True
