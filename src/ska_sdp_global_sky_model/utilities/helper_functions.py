"""This module contains helper functions for the ska_sdp_global_sky_model"""

import json


def calculate_percentage(dividend: int | float, divisor: int | float) -> float:
    """
    Calculates the percentage that the `dividend` represents of the `divisor`
    (dividend/divisor)*100. If the `divisor` is zero the ZeroDivisionError is swallowed and
    it returns 0.0.

    Args:
        dividend (int|float): The value we want to express as a percentage of the total.
        divisor (int|float): The total value of which the `dividend` is a part.

    Returns:
        float: The percentage value between 0 and 100, rounded to two decimal places.

    Raises:
        ZeroDivisionError: If the `divisor` is zero.
    """
    if divisor == 0:
        # Handle division by zero case
        return 0.0
    percentage = (dividend / divisor) * 100
    return round(percentage, 2)  # Round to two decimal places


def make_serisalisable(obj):
    """
    Ensures that an object is JSON serializable.

    Args:
        obj (Any): The object to be checked for JSON serialization.

    Returns:
        Any: The original object if it is JSON serializable, otherwise its
        string representation.
    """

    try:
        json.dumps(obj)
        return obj
    except TypeError:
        return str(obj)
