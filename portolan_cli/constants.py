"""Shared constants for the Portolan CLI.

This module contains constants that are used across multiple modules
to avoid duplication and ensure consistency.
"""

from __future__ import annotations

# Extensions we recognize as geospatial files
# Note: .gdb is a directory extension (FileGDB) - handled specially in detection code
GEOSPATIAL_EXTENSIONS: frozenset[str] = frozenset(
    {
        ".geojson",
        ".parquet",
        ".shp",
        ".gpkg",
        ".fgb",
        ".gdb",  # FileGDB directory (ESRI File Geodatabase)
        ".csv",
        ".tsv",  # Tab-separated values (may or may not have geometry)
        ".tif",
        ".tiff",
        ".jp2",
        ".pmtiles",
    }
)

# Extensions for tabular data that may or may not have geometry columns.
# When these files lack geometry, they should be tracked as non-geospatial assets
# (per ADR-0028) rather than causing errors.
TABULAR_EXTENSIONS: frozenset[str] = frozenset({".csv", ".tsv"})

# Cloud-native parquet extension
PARQUET_EXTENSION: str = ".parquet"

# Sidecar file patterns by primary file extension
SIDECAR_PATTERNS: dict[str, list[str]] = {
    ".shp": [".dbf", ".shx", ".prj", ".cpg", ".sbn", ".sbx", ".qix"],
    ".tif": [".tfw", ".xml", ".aux.xml", ".ovr"],
    ".tiff": [".tfw", ".xml", ".aux.xml", ".ovr"],
    ".img": [".ige", ".rrd", ".rde", ".xml"],
}

# Change detection constants (per ADR-0017)
# 2 second tolerance for NFS/CIFS compatibility where mtime resolution is coarse
MTIME_TOLERANCE_SECONDS: float = 2.0

# The .portolan directory name (Portolan internal metadata directory)
PORTOLAN_DIR: str = ".portolan"

# Maximum depth for catalog root discovery (prevent traversing to filesystem root)
MAX_CATALOG_SEARCH_DEPTH: int = 20

# Windows reserved device names (case-insensitive)
# Files with these names (with any extension) are problematic on Windows.
# Used by scan.py and scan_fix.py for cross-platform compatibility checks.
WINDOWS_RESERVED_NAMES: frozenset[str] = frozenset(
    {
        "con",
        "prn",
        "aux",
        "nul",
        "com1",
        "com2",
        "com3",
        "com4",
        "com5",
        "com6",
        "com7",
        "com8",
        "com9",
        "lpt1",
        "lpt2",
        "lpt3",
        "lpt4",
        "lpt5",
        "lpt6",
        "lpt7",
        "lpt8",
        "lpt9",
    }
)

# ISO 3166-1 alpha-3 country codes (vendored from FAO GAUL 2024)
# Used to suppress uppercase warnings for valid country code directory names.
# See ADR-0030 for rationale and maintenance notes.
# Source: https://data.apps.fao.org/catalog/dataset/iso-3-code-list-global-region-country
ISO_ALPHA3_CODES: frozenset[str] = frozenset(
    {
        "ABW",
        "AFG",
        "AGO",
        "AIA",
        "ALA",
        "ALB",
        "AND",
        "ARE",
        "ARG",
        "ARM",
        "ASM",
        "ATA",
        "ATF",
        "ATG",
        "AUS",
        "AUT",
        "AZE",
        "BDI",
        "BEL",
        "BEN",
        "BES",
        "BFA",
        "BGD",
        "BGR",
        "BHR",
        "BHS",
        "BIH",
        "BLM",
        "BLR",
        "BLZ",
        "BMU",
        "BOL",
        "BRA",
        "BRB",
        "BRN",
        "BTN",
        "BVT",
        "BWA",
        "CAF",
        "CAN",
        "CCK",
        "CHE",
        "CHL",
        "CHN",
        "CIV",
        "CMR",
        "COD",
        "COG",
        "COK",
        "COL",
        "COM",
        "CPV",
        "CRI",
        "CUB",
        "CUW",
        "CXR",
        "CYM",
        "CYP",
        "CZE",
        "DEU",
        "DJI",
        "DMA",
        "DNK",
        "DOM",
        "DZA",
        "ECU",
        "EGY",
        "ERI",
        "ESH",
        "ESP",
        "EST",
        "ETH",
        "FIN",
        "FJI",
        "FLK",
        "FRA",
        "FRO",
        "FSM",
        "GAB",
        "GBR",
        "GEO",
        "GGY",
        "GHA",
        "GIB",
        "GIN",
        "GLP",
        "GMB",
        "GNB",
        "GNQ",
        "GRC",
        "GRD",
        "GRL",
        "GTM",
        "GUF",
        "GUM",
        "GUY",
        "HKG",
        "HMD",
        "HND",
        "HRV",
        "HTI",
        "HUN",
        "IDN",
        "IMN",
        "IND",
        "IOT",
        "IRL",
        "IRN",
        "IRQ",
        "ISL",
        "ISR",
        "ITA",
        "JAM",
        "JEY",
        "JOR",
        "JPN",
        "KAZ",
        "KEN",
        "KGZ",
        "KHM",
        "KIR",
        "KNA",
        "KOR",
        "KWT",
        "LAO",
        "LBN",
        "LBR",
        "LBY",
        "LCA",
        "LIE",
        "LKA",
        "LSO",
        "LTU",
        "LUX",
        "LVA",
        "MAC",
        "MAF",
        "MAR",
        "MCO",
        "MDA",
        "MDG",
        "MDV",
        "MEX",
        "MHL",
        "MKD",
        "MLI",
        "MLT",
        "MMR",
        "MNE",
        "MNG",
        "MNP",
        "MOZ",
        "MRT",
        "MSR",
        "MTQ",
        "MUS",
        "MWI",
        "MYS",
        "MYT",
        "NAM",
        "NCL",
        "NER",
        "NFK",
        "NGA",
        "NIC",
        "NIU",
        "NLD",
        "NOR",
        "NPL",
        "NRU",
        "NZL",
        "OMN",
        "PAK",
        "PAN",
        "PCN",
        "PER",
        "PHL",
        "PLW",
        "PNG",
        "POL",
        "PRI",
        "PRK",
        "PRT",
        "PRY",
        "PSE",
        "PYF",
        "QAT",
        "REU",
        "ROU",
        "RUS",
        "RWA",
        "SAU",
        "SDN",
        "SEN",
        "SGP",
        "SGS",
        "SHN",
        "SJM",
        "SLB",
        "SLE",
        "SLV",
        "SMR",
        "SOM",
        "SPM",
        "SRB",
        "SSD",
        "STP",
        "SUR",
        "SVK",
        "SVN",
        "SWE",
        "SWZ",
        "SXM",
        "SYC",
        "SYR",
        "TCA",
        "TCD",
        "TGO",
        "THA",
        "TJK",
        "TKL",
        "TKM",
        "TLS",
        "TON",
        "TTO",
        "TUN",
        "TUR",
        "TUV",
        "TWN",
        "TZA",
        "UGA",
        "UKR",
        "UMI",
        "URY",
        "USA",
        "UZB",
        "VAT",
        "VCT",
        "VEN",
        "VGB",
        "VIR",
        "VNM",
        "VUT",
        "WLF",
        "WSM",
        "YEM",
        "ZAF",
        "ZMB",
        "ZWE",
    }
)


def is_valid_uppercase_id(name: str) -> bool:
    """Check if an uppercase name is a known ISO code or disputed territory.

    Used by scan validation to suppress uppercase warnings for valid
    geospatial naming conventions.

    Args:
        name: The directory/collection name to check.

    Returns:
        True if the name is a valid ISO 3166-1 alpha-3 code or follows
        the disputed territory pattern (x + 2 uppercase letters).
    """
    # Standard ISO country code
    if name in ISO_ALPHA3_CODES:
        return True
    # Disputed territory pattern: x + 2 uppercase letters (e.g., xAB, xJK)
    # Used in FAO GAUL and other geospatial datasets
    if len(name) == 3 and name[0] == "x" and name[1:].isupper() and name[1:].isalpha():
        return True
    return False
