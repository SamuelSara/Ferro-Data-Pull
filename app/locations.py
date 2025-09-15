"""Location utilities for ERCOT zones and hubs."""
from __future__ import annotations

from typing import Dict, Optional

CANONICAL_LOCATIONS: Dict[str, str] = {
    "NORTH": "NORTH",
    "SOUTH": "SOUTH",
    "HOUSTON": "HOUSTON",
    "WEST": "WEST",
    "HB_NORTH": "HB_NORTH",
    "HB_SOUTH": "HB_SOUTH",
    "HB_HOUSTON": "HB_HOUSTON",
    "HB_WEST": "HB_WEST",
}

LOCATION_ALIASES = {
    "NORTH ZONE": "NORTH",
    "SOUTH ZONE": "SOUTH",
    "HOUSTON ZONE": "HOUSTON",
    "WEST ZONE": "WEST",
    "LZ_NORTH": "NORTH",
    "LZ_SOUTH": "SOUTH",
    "LZ_HOUSTON": "HOUSTON",
    "LZ_WEST": "WEST",
    "HB NORTH": "HB_NORTH",
    "HB SOUTH": "HB_SOUTH",
    "HB HOUSTON": "HB_HOUSTON",
    "HB WEST": "HB_WEST",
    "HB_NORTH_HUB": "HB_NORTH",
    "HB_SOUTH_HUB": "HB_SOUTH",
    "HB_HOUSTON_HUB": "HB_HOUSTON",
    "HB_WEST_HUB": "HB_WEST",
    "NORTH HUB": "HB_NORTH",
    "SOUTH HUB": "HB_SOUTH",
    "HOUSTON HUB": "HB_HOUSTON",
    "WEST HUB": "HB_WEST",
}


def normalize_location(raw: str | None) -> Optional[str]:
    if raw is None:
        return None

    value = str(raw).strip().upper()
    value = value.replace("-", "_").replace(" ", "_")

    if value in CANONICAL_LOCATIONS:
        return value

    if value in LOCATION_ALIASES:
        return LOCATION_ALIASES[value]

    for prefix in ("LZ_", "HZ_", "HZON_", "LOAD_ZONE_", "HB_"):
        if value.startswith(prefix):
            candidate = value[len(prefix) :]
            if candidate in CANONICAL_LOCATIONS:
                return candidate
            if candidate in LOCATION_ALIASES:
                return LOCATION_ALIASES[candidate]

    return None
