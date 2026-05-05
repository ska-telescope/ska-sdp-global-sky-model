"""Test CLI scripts."""

from datetime import datetime
from unittest.mock import ANY, call, patch

import pytest

from ska_sdp_global_sky_model.api.app.models import (
    GlobalSkyModelMetadata,
    SkyComponent,
    SkyComponentStaging,
)
from ska_sdp_global_sky_model.cli.catalogue_delete import main as delete_main
from ska_sdp_global_sky_model.cli.cleanup import main as cleanup_main
from tests.utils import clean_all_tables, override_get_db


@pytest.fixture(scope="function", autouse=True)
def clean_up_database():
    """
    Clean tables after each test run.
    Specific to this module. Do not move.
    """
    yield
    clean_all_tables()


def test_cleanup_help(capsys):
    """Test the help function"""
    with pytest.raises(SystemExit) as error:
        cleanup_main(["--help"])

    assert error.value.code == 0
    assert (
        ("\n".join(capsys.readouterr().out.rstrip().split("\n")[:-2]))
        == """usage: pytest [-h] [--max-age MAX_AGE] [--delete]

Run cleanups of the catalogues

options:
  -h, --help         show this help message and exit
  --max-age MAX_AGE  The maximum age of staged catalogues in hours (default:
                     168)
  --delete           Commit the deletion (default: False)"""
    )


def test_delete_help(capsys):
    """Test the help function"""
    with pytest.raises(SystemExit) as error:
        delete_main(["--help"])

    assert error.value.code == 0
    assert (
        ("\n".join(capsys.readouterr().out.rstrip().split("\n")[:-2]))
        == """usage: pytest [-h] [--delete] catalogue_id

Delete a catalogue, this can delete a staging or released catalogue.

positional arguments:
  catalogue_id  The numerical ID of a catalogue

options:
  -h, --help    show this help message and exit
  --delete      Do the actual delete (default: False)"""
    )


@patch("ska_sdp_global_sky_model.cli.cleanup.UploadManager")
def test_cleanup_calls_function(mock_upload_manager):
    """Check that the default params are sent"""
    cleanup_main([])

    assert mock_upload_manager.mock_calls == [call(), call().run_db_cleanup(ANY, False, 168)]


def test_cleanup_negative_age(capsys):
    """Check that the max age is positive"""
    with pytest.raises(SystemExit) as error:
        cleanup_main(["--max-age", "-1"])

    assert error.value.code == 2
    assert "error: MAX_AGE must be positive" in capsys.readouterr().err


@patch("ska_sdp_global_sky_model.cli.cleanup.UploadManager")
def test_cleanup_calls_function_with_delete(mock_upload_manager):
    """Check that the delete option is sent to the cleanup function"""
    cleanup_main(["--delete"])

    assert mock_upload_manager.mock_calls == [call(), call().run_db_cleanup(ANY, True, 168)]


@patch("ska_sdp_global_sky_model.cli.cleanup.UploadManager")
def test_cleanup_calls_function_with_max_age(mock_upload_manager):
    """Check that the new age is sent to the cleanup function"""
    cleanup_main(["--max-age", "24"])

    assert mock_upload_manager.mock_calls == [call(), call().run_db_cleanup(ANY, False, 24)]


def test_delete_fails_with_no_args(capsys):
    """Check that the command fails when no arguments are parsed"""
    with pytest.raises(SystemExit) as error:
        delete_main([])

    assert error.value.code == 2
    assert "error: the following arguments are required: catalogue_id" in capsys.readouterr().err


@patch("ska_sdp_global_sky_model.cli.catalogue_delete.get_db")
def test_delete_no_delete(mock_get_db, db_session):
    """Check that without the delete flag, nothing is deleted"""
    mock_get_db.side_effect = override_get_db
    catalogue = GlobalSkyModelMetadata(
        catalogue_name="Old Catalogue",
        upload_id="1234-abcd-c",
        staging=True,
        uploaded_at=(datetime.now()),
    )
    db_session.add(catalogue)
    db_session.commit()
    component = SkyComponentStaging(
        component_id="C1", ra_deg=1, dec_deg=1, upload_id="1234-abcd", gsm_id=catalogue.id
    )
    db_session.add(component)
    db_session.commit()

    delete_main([f"{catalogue.id}"])

    assert db_session.query(GlobalSkyModelMetadata).count() == 1
    assert db_session.query(SkyComponentStaging).count() == 1


@patch("ska_sdp_global_sky_model.cli.catalogue_delete.get_db")
def test_delete_not_exist(mock_get_db):
    """Check that the command fails if the catalogue doesn't exist"""
    mock_get_db.side_effect = override_get_db

    with patch("logging.Logger.error") as log_error:
        with pytest.raises(SystemExit) as error:
            delete_main(["--delete", "100"])

    assert error.value.code == 1
    assert any(call("No catalogues found for ID '%s'", 100) == c for c in log_error.call_args_list)


@patch("ska_sdp_global_sky_model.cli.catalogue_delete.get_db")
def test_delete_staging(mock_get_db, db_session):
    """Check that a staging catalogue is deleted"""
    mock_get_db.side_effect = override_get_db
    catalogue = GlobalSkyModelMetadata(
        catalogue_name="Old Catalogue",
        upload_id="1234-abcd-c",
        staging=True,
        uploaded_at=(datetime.now()),
    )
    db_session.add(catalogue)
    db_session.commit()
    component = SkyComponentStaging(
        component_id="C1", ra_deg=1, dec_deg=1, upload_id="1234-abcd", gsm_id=catalogue.id
    )
    db_session.add(component)
    db_session.commit()

    delete_main(["--delete", f"{catalogue.id}"])

    assert db_session.query(GlobalSkyModelMetadata).count() == 0
    assert db_session.query(SkyComponentStaging).count() == 0
    assert db_session.query(SkyComponent).count() == 0


@patch("ska_sdp_global_sky_model.cli.catalogue_delete.get_db")
def test_delete_released(mock_get_db, db_session):
    """Check that a released catalogue is delete"""
    mock_get_db.side_effect = override_get_db
    catalogue = GlobalSkyModelMetadata(
        catalogue_name="Old Catalogue",
        upload_id="1234-abcd-c",
        staging=False,
        uploaded_at=(datetime.now()),
    )
    db_session.add(catalogue)
    db_session.commit()
    component = SkyComponent(component_id="C1", ra_deg=1, dec_deg=1, gsm_id=catalogue.id)
    db_session.add(component)
    db_session.commit()

    delete_main(["--delete", f"{catalogue.id}"])

    assert db_session.query(GlobalSkyModelMetadata).count() == 0
    assert db_session.query(SkyComponentStaging).count() == 0
    assert db_session.query(SkyComponent).count() == 0


@patch("ska_sdp_global_sky_model.cli.catalogue_delete.get_db")
def test_delete_partial_staging(mock_get_db, db_session):
    """Check that a staging catalogue that is in both tables, are cleaned"""
    mock_get_db.side_effect = override_get_db
    catalogue = GlobalSkyModelMetadata(
        catalogue_name="Old Catalogue",
        upload_id="1234-abcd-c",
        staging=True,
        uploaded_at=(datetime.now()),
    )
    db_session.add(catalogue)
    db_session.commit()
    component = SkyComponentStaging(
        component_id="C1", ra_deg=1, dec_deg=1, upload_id="1234-abcd", gsm_id=catalogue.id
    )
    db_session.add(component)
    component = SkyComponent(component_id="C1", ra_deg=1, dec_deg=1, gsm_id=catalogue.id)
    db_session.add(component)
    db_session.commit()

    delete_main(["--delete", f"{catalogue.id}"])

    assert db_session.query(GlobalSkyModelMetadata).count() == 0
    assert db_session.query(SkyComponentStaging).count() == 0
    assert db_session.query(SkyComponent).count() == 0


@patch("ska_sdp_global_sky_model.cli.catalogue_delete.get_db")
def test_delete_partial_released(mock_get_db, db_session):
    """Check that a released catalogue that is in both tables, are cleaned"""
    mock_get_db.side_effect = override_get_db
    catalogue = GlobalSkyModelMetadata(
        catalogue_name="Old Catalogue",
        upload_id="1234-abcd-c",
        staging=False,
        uploaded_at=(datetime.now()),
    )
    db_session.add(catalogue)
    db_session.commit()
    component = SkyComponentStaging(
        component_id="C1", ra_deg=1, dec_deg=1, upload_id="1234-abcd", gsm_id=catalogue.id
    )
    db_session.add(component)
    component = SkyComponent(component_id="C1", ra_deg=1, dec_deg=1, gsm_id=catalogue.id)
    db_session.add(component)
    db_session.commit()

    delete_main(["--delete", f"{catalogue.id}"])

    assert db_session.query(GlobalSkyModelMetadata).count() == 0
    assert db_session.query(SkyComponentStaging).count() == 0
    assert db_session.query(SkyComponent).count() == 0
