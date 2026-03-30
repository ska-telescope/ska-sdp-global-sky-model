"""Test the feature toggle class"""

import pytest

from ska_sdp_global_sky_model.utilities.feature_toggle import FeatureToggle


def test_basic_usage():
    feature_toggle = FeatureToggle("MY_FEATURE", False)

    assert feature_toggle._key == "FEATURE_MY_FEATURE"
    assert feature_toggle._default is False

    assert feature_toggle._name == "MY_FEATURE"

    assert feature_toggle.is_active() is False


@pytest.mark.parametrize(
    "env_val, expected_result",
    [
        ("", False),
        ("1", True),
        ("yes", True),
        ("true", True),
        ("True", True),
        ("Yes", True),
        ("0", False),
        ("no", False),
        ("false", False),
        ("False", False),
        ("No", False),
    ],
)
def test_feature_enabled(monkeypatch, env_val, expected_result):
    monkeypatch.setenv("FEATURE_MY_FEATURE", env_val)

    feature_toggle = FeatureToggle("MY_FEATURE", False)
    assert feature_toggle.is_active() == expected_result
