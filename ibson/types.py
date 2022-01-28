"""types.py.

Custom Types for BSON.
"""
from ibson.codec_util import (
    INT32_UPPERBOUND, INT32_LOWERBOUND,
    INT64_UPPERBOUND, INT64_LOWERBOUND,
    UINT32_UPPERBOUND,
)


class MinKey(object):
    """Default class representing the "Min key" field in BSON."""


class MaxKey(object):
    """Default class representing the "Max key" field in BSON."""


class Int32(int):
    """Type used to force BSON serialization as an Int32.

    This type should work interchangeably as an int for other operations.
    """

    def __new__(cls, *args, **kwargs):
        """Override 'new' operation since ints are immutable types."""
        val = super(Int32, cls).__new__(cls, *args, **kwargs)
        if val < INT32_LOWERBOUND:
            raise TypeError(
                "Int32 cannot store values less than {}! Value: {}".format(
                    INT32_LOWERBOUND, val))
        if val > INT32_UPPERBOUND:
            raise TypeError(
                "Int32 cannot store values larger than {}! Value: {}".format(
                    INT32_UPPERBOUND, val))
        return val


class Int64(int):
    """Type used to force BSON serialization as an Int32.

    This type should work interchangeably as an int for other operations.
    """

    def __new__(cls, *args, **kwargs):
        """Override 'new' operation since ints are immutable types."""
        val = super(Int32, cls).__new__(cls, *args, **kwargs)
        if val < INT64_LOWERBOUND:
            raise TypeError(
                "Int32 cannot store values less than {}! Value: {}".format(
                    INT64_LOWERBOUND, val))
        if val > INT64_UPPERBOUND:
            raise TypeError(
                "Int32 cannot store values larger than {}! Value: {}".format(
                    INT64_UPPERBOUND, val))
        return val


class UInt32(int):
    """Type used to force BSON serialization as a UInt32.

    This type should work interchangeably as an int for other operations.
    """

    def __new__(cls, *args, **kwargs):
        """Override 'new' operation since ints are immutable types."""
        val = super(UInt32, cls).__new__(cls, *args, **kwargs)
        if val < 0:
            raise TypeError(
                "UInt32 values cannot be negative! Value given: {}".format(val))
        if val > UINT32_UPPERBOUND:
            raise TypeError(
                "UInt32 cannot store values larger than {}! Value: {}".format(
                    UINT32_UPPERBOUND, val))
        return val
