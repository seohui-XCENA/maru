"""Shared data types for the marufs package."""

from dataclasses import dataclass


@dataclass
class RegionInfo:
    """Metadata about an marufs region (file).

    Attributes:
        name:  Region filename within the marufs mount point.
        fd:    Open file descriptor for the region.
        size:  Size of the region in bytes.
        owned: True if this client created the region (vs. opened an existing one).
    """

    name: str
    fd: int
    size: int
    owned: bool  # True if this client created the region
