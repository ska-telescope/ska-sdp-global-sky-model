"""Feature toggle."""

import os


class FeatureToggle:
    """Feature toggle."""

    def __init__(self, name: str, default: bool):
        """Initialise feature toggle.

        :param name: Name of feature.
        :param default: Default value for toggle.

        """
        self._name = name
        self._default = default
        self._key = str("feature_" + self._name).upper()

    def set_default(self, default: bool) -> None:
        """Set feature default toggle value.

        :param default: Default value for toggle.

        """
        self._default = default

    def is_active(self) -> bool:
        """Check if feature is active.

        :returns: Toggle value.

        """
        if self._key in os.environ:
            return os.environ.get(self._key, "")[:1].upper() in ("1", "T", "Y")
        return self._default
