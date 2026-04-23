"""Configuration management for Portolan catalogs.

This module provides hierarchical configuration with the following precedence
(highest to lowest):
1. CLI argument
2. Environment variable (PORTOLAN_<KEY>)
3. Collection-level config
4. Catalog-level config
5. Built-in default (None)

Config is stored in `.portolan/config.yaml` (see ADR-0024).

Usage:
    from portolan_cli.config import get_setting, set_setting, load_config

    # Get a setting with full precedence resolution
    remote = get_setting("remote", cli_value=cli_remote, catalog_path=catalog_path)

    # Set a catalog-level setting
    set_setting(catalog_path, "remote", "s3://bucket/")

    # Set a collection-level setting
    set_setting(catalog_path, "aws_profile", "special", collection="restricted")
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

# Sensitive settings that must NOT be stored in config.yaml (Issue #356)
# These get pushed to remote storage and would expose credentials/infra details.
# Use environment variables (PORTOLAN_<KEY>) or .env files instead.
SENSITIVE_SETTINGS: frozenset[str] = frozenset(
    {
        "remote",
        "aws_profile",
        "profile",
        "region",
    }
)

# Known settings for documentation/validation (but unknown keys are still allowed)
KNOWN_SETTINGS: frozenset[str] = frozenset(
    {
        "ignored_files",
        "backend",
        "statistics.enabled",
        "statistics.raster_mode",
        "parquet.enabled",  # Generate items.parquet for large collections
        "parquet.threshold",  # Item count threshold to suggest parquet generation
        "pmtiles.enabled",  # Generate PMTiles for GeoParquet collections
        "pmtiles.min_zoom",  # Minimum zoom level (None = auto-detect)
        "pmtiles.max_zoom",  # Maximum zoom level (None = auto-detect)
        "pmtiles.layer",  # Layer name in PMTiles (defaults to filename)
        "pmtiles.bbox",  # Bounding box filter as "minx,miny,maxx,maxy"
        "pmtiles.where",  # SQL WHERE clause for filtering features
        "pmtiles.include_cols",  # Comma-separated columns to include in tiles
        "pmtiles.precision",  # Coordinate decimal precision (default: 6)
        "pmtiles.attribution",  # Attribution HTML for tiles
        "pmtiles.src_crs",  # Override source CRS if metadata is incorrect
    }
)

# Setting aliases: maps canonical key -> list of alternative keys to check
# When looking up aws_profile, also check for "profile" in the config
SETTING_ALIASES: dict[str, list[str]] = {
    "aws_profile": ["profile"],
}

# Default values for settings (per ADR-0034 for statistics)
DEFAULT_SETTINGS: dict[str, Any] = {
    "statistics.enabled": True,
    "statistics.raster_mode": "approx",
    "parquet.enabled": False,  # Disabled by default (100% optional per issue #319)
    "parquet.threshold": 100,  # Suggest parquet generation when items > threshold
    "pmtiles.enabled": False,  # Disabled by default (requires tippecanoe)
    "pmtiles.min_zoom": None,  # None = tippecanoe auto-detection
    "pmtiles.max_zoom": None,  # None = tippecanoe auto-detection
    "pmtiles.layer": None,  # None = use output filename
    "pmtiles.bbox": None,  # None = no bounding box filter
    "pmtiles.where": None,  # None = no SQL filter
    "pmtiles.include_cols": None,  # None = include all columns
    "pmtiles.precision": 6,  # Coordinate decimal precision
    "pmtiles.attribution": None,  # None = gpio-pmtiles default
    "pmtiles.src_crs": None,  # None = use metadata CRS
}

# Default glob patterns for files to exclude from asset tracking (per ADR-0028).
# These cover common OS-generated junk files and temporary files that should
# never appear as STAC assets. Users can override this list in config.yaml.
DEFAULT_IGNORED_FILES: list[str] = [
    ".DS_Store",  # macOS Finder metadata
    "Thumbs.db",  # Windows thumbnail cache
    "desktop.ini",  # Windows folder settings
    "*.tmp",  # Temporary files
    "*.temp",  # Temporary files (alternate extension)
    "~*",  # Temporary files (e.g., ~lock.docx)
    ".git*",  # Git internals (.gitignore, .gitattributes, etc.)
    "*.pyc",  # Python bytecode
    "__pycache__",  # Python cache directory marker
    ".env",  # Environment files with credentials (Issue #356)
    ".env.*",  # .env.local, .env.production, etc.
    ".env.local",  # Explicit for common pattern
]

# Config file name (inside .portolan/)
CONFIG_FILENAME = "config.yaml"


def get_config_path(catalog_path: Path) -> Path:
    """Get the path to the config file for a catalog.

    Args:
        catalog_path: Root path of the catalog.

    Returns:
        Path to .portolan/config.yaml
    """
    return catalog_path / ".portolan" / CONFIG_FILENAME


def load_config(catalog_path: Path) -> dict[str, Any]:
    """Load configuration from .portolan/config.yaml.

    Args:
        catalog_path: Root path of the catalog.

    Returns:
        Config dictionary. Returns empty dict if file doesn't exist.
    """
    config_file = get_config_path(catalog_path)

    if not config_file.exists():
        return {}

    content = config_file.read_text()
    if not content.strip():
        return {}

    try:
        data = yaml.safe_load(content)
    except yaml.YAMLError as e:
        from portolan_cli.errors import ConfigParseError

        raise ConfigParseError(str(config_file), str(e)) from e

    if data is None:
        return {}

    if not isinstance(data, dict):
        from portolan_cli.errors import ConfigInvalidStructureError

        raise ConfigInvalidStructureError(str(config_file), "Config must be a YAML mapping")

    # Validate collections section if present
    if "collections" in data and not isinstance(data["collections"], dict):
        from portolan_cli.errors import ConfigInvalidStructureError

        raise ConfigInvalidStructureError(str(config_file), "'collections' must be a mapping")

    return data


def get_ignored_files(catalog_path: Path | None) -> list[str]:
    """Return the list of glob patterns for files to exclude from asset tracking.

    Reads the ``ignored_files`` key from .portolan/config.yaml.  If the key is
    absent (or no catalog_path is given) the built-in DEFAULT_IGNORED_FILES list
    is returned so that callers always get a usable value without any config
    being present.

    The config value **replaces** the defaults entirely — if a user sets
    ``ignored_files``, they take full control over the list.  This matches how
    ``.gitignore`` works: a file at a given scope replaces inherited patterns.

    Args:
        catalog_path: Root path of the catalog, or None to get defaults.

    Returns:
        List of glob patterns (strings).  Never returns None.

    Raises:
        ConfigInvalidStructureError: If ``ignored_files`` is present but not a
            list of strings.
    """
    if catalog_path is None:
        return list(DEFAULT_IGNORED_FILES)

    config = load_config(catalog_path)

    if "ignored_files" not in config:
        return list(DEFAULT_IGNORED_FILES)

    raw = config["ignored_files"]

    if not isinstance(raw, list):
        from portolan_cli.errors import ConfigInvalidStructureError

        raise ConfigInvalidStructureError(
            str(get_config_path(catalog_path)),
            "'ignored_files' must be a list of glob patterns (strings)",
        )

    for item in raw:
        if not isinstance(item, str):
            from portolan_cli.errors import ConfigInvalidStructureError

            raise ConfigInvalidStructureError(
                str(get_config_path(catalog_path)),
                f"'ignored_files' must be a list of strings; got item {item!r}",
            )

    return list(raw)


def save_config(catalog_path: Path, config: dict[str, Any]) -> None:
    """Save configuration to .portolan/config.yaml.

    Creates the .portolan directory if it doesn't exist.

    Args:
        catalog_path: Root path of the catalog.
        config: Config dictionary to save.
    """
    portolan_dir = catalog_path / ".portolan"
    portolan_dir.mkdir(parents=True, exist_ok=True)

    config_file = portolan_dir / CONFIG_FILENAME

    # Use default_flow_style=False for readable multi-line YAML
    content = yaml.safe_dump(config, default_flow_style=False, sort_keys=False)
    config_file.write_text(content)


def _get_env_var_name(key: str) -> str:
    """Convert a setting key to environment variable name.

    Normalizes the key by replacing hyphens and other non-alphanumeric
    characters with underscores.

    Args:
        key: Setting key (e.g., "aws_profile", "max-depth")

    Returns:
        Environment variable name (e.g., "PORTOLAN_AWS_PROFILE", "PORTOLAN_MAX_DEPTH")
    """
    import re

    # Replace non-alphanumeric chars with underscores, then uppercase
    normalized = re.sub(r"[^A-Za-z0-9]", "_", key)
    return f"PORTOLAN_{normalized.upper()}"


def get_setting(
    key: str,
    cli_value: Any | None = None,
    catalog_path: Path | None = None,
    collection: str | None = None,
    collection_path: Path | None = None,
) -> Any | None:
    """Resolve a setting with full precedence.

    Precedence (highest to lowest):
    1. CLI argument (cli_value)
    2. Environment variable (PORTOLAN_<KEY>)
    3. Hierarchical .portolan/ config (if collection_path specified, ADR-0039)
    4. Legacy collection-level config (collections: section, if collection specified)
    5. Catalog-level config
    6. Built-in default (None)

    Args:
        key: Setting key (e.g., "remote", "aws_profile")
        cli_value: Value passed via CLI argument (highest precedence)
        catalog_path: Path to catalog root for loading config file
        collection: Optional collection name for legacy collection-level config
        collection_path: Optional path to collection directory for hierarchical lookup

    Returns:
        Resolved value, or None if not found at any level.
    """
    # 1. CLI argument takes highest precedence
    if cli_value is not None:
        return cli_value

    # 2. Environment variable (skip empty strings)
    # Check primary env var and alias-derived env vars
    env_var = _get_env_var_name(key)
    env_value = os.environ.get(env_var)
    if env_value:  # Non-empty string
        return env_value
    # Check aliases (e.g., aws_profile aliases to profile, so check PORTOLAN_PROFILE)
    for alias in SETTING_ALIASES.get(key, []):
        alias_env_var = _get_env_var_name(alias)
        alias_env_value = os.environ.get(alias_env_var)
        if alias_env_value:
            return alias_env_value

    # If no catalog path, can't check file-based config
    if catalog_path is None:
        return None

    # Helper to check key and its aliases in a config dict
    def _get_with_aliases(cfg: dict[str, Any], k: str) -> Any | None:
        if k in cfg:
            return cfg[k]
        # Check aliases (e.g., aws_profile -> profile)
        for alias in SETTING_ALIASES.get(k, []):
            if alias in cfg:
                return cfg[alias]
        return None

    # Helper to check if sensitive key exists in config and raise error
    def _check_sensitive_in_config(cfg: dict[str, Any], k: str) -> None:
        if k in SENSITIVE_SETTINGS:
            if k in cfg or any(a in cfg for a in SETTING_ALIASES.get(k, [])):
                env_var_name = _get_env_var_name(k)
                raise ValueError(
                    f"'{k}' found in config.yaml but sensitive settings cannot be read from "
                    f"config files. Use environment variable {env_var_name} or .env file."
                )

    # 3. Hierarchical .portolan/ config (ADR-0039)
    if collection_path is not None:
        merged_config = load_merged_config(collection_path, catalog_path)
        _check_sensitive_in_config(merged_config, key)
        value = _get_with_aliases(merged_config, key)
        if value is not None:
            return value
        # Fall through to built-in defaults

    else:
        # Legacy behavior: use collections: section in root config
        # Load config from file
        config = load_config(catalog_path)

        # 4. Legacy collection-level config (if collection specified)
        if collection is not None:
            collections = config.get("collections", {})
            collection_config = collections.get(collection, {})
            _check_sensitive_in_config(collection_config, key)
            value = _get_with_aliases(collection_config, key)
            if value is not None:
                return value

        # 5. Catalog-level config
        _check_sensitive_in_config(config, key)
        value = _get_with_aliases(config, key)
        if value is not None:
            return value

    # 6. Built-in default from DEFAULT_SETTINGS
    return DEFAULT_SETTINGS.get(key)


def set_setting(
    catalog_path: Path,
    key: str,
    value: Any,
    collection: str | None = None,
) -> None:
    """Set a configuration value.

    Creates the config file and .portolan directory if they don't exist.

    Args:
        catalog_path: Root path of the catalog.
        key: Setting key (e.g., "ignored_files", "statistics.enabled")
        value: Value to set
        collection: Optional collection name for collection-level config

    Raises:
        ValueError: If key is a sensitive setting (remote, profile, region).
            These must be set via environment variables or .env files.
    """
    if key in SENSITIVE_SETTINGS:
        env_var = _get_env_var_name(key)
        raise ValueError(
            f"'{key}' cannot be stored in config.yaml (would be pushed to remote). "
            f"Use environment variable {env_var} or add to .env file in catalog root."
        )

    config = load_config(catalog_path)

    if collection is not None:
        # Set collection-level config
        if "collections" not in config:
            config["collections"] = {}
        if collection not in config["collections"]:
            config["collections"][collection] = {}
        config["collections"][collection][key] = value
    else:
        # Set catalog-level config
        config[key] = value

    save_config(catalog_path, config)


def unset_setting(
    catalog_path: Path,
    key: str,
    collection: str | None = None,
) -> bool:
    """Remove a configuration value.

    Args:
        catalog_path: Root path of the catalog.
        key: Setting key to remove
        collection: Optional collection name for collection-level config

    Returns:
        True if the key existed and was removed, False if key didn't exist.
    """
    config = load_config(catalog_path)

    if collection is not None:
        # Remove from collection-level config
        collections = config.get("collections", {})
        collection_config = collections.get(collection, {})
        if key not in collection_config:
            return False
        del collection_config[key]
    else:
        # Remove from catalog-level config
        if key not in config:
            return False
        del config[key]

    save_config(catalog_path, config)
    return True


def list_settings(
    catalog_path: Path | None = None,
    collection: str | None = None,
) -> dict[str, dict[str, Any]]:
    """List all settings with their sources.

    Returns a dictionary mapping setting keys to their resolved values
    and sources (cli, env, collection, catalog, default).

    Args:
        catalog_path: Path to catalog root for loading config file.
        collection: Optional collection name to include collection-level config.

    Returns:
        Dict mapping setting keys to {"value": ..., "source": ...}
    """
    result: dict[str, dict[str, Any]] = {}

    # Load file-based config
    config = load_config(catalog_path) if catalog_path else {}

    # Get all keys from config file
    all_keys = set(config.keys()) - {"collections"}

    # Add collection keys if specified
    if collection and "collections" in config:
        collection_config = config.get("collections", {}).get(collection, {})
        all_keys.update(collection_config.keys())

    # Add known settings
    all_keys.update(KNOWN_SETTINGS)

    # Check environment variables for all known + sensitive settings
    # Sensitive settings can still be read from env vars, just not saved to config
    for key in KNOWN_SETTINGS | SENSITIVE_SETTINGS:
        env_var = _get_env_var_name(key)
        if env_var in os.environ:
            all_keys.add(key)

    # Resolve each setting
    for key in sorted(all_keys):
        try:
            value = get_setting(key, catalog_path=catalog_path, collection=collection)
            source = get_setting_source(key, catalog_path, collection)
            if value is not None or source != "default":
                result[key] = {"value": value, "source": source}
        except ValueError:
            # Sensitive key in config without env var - show with warning source
            # Get the value directly from config for display purposes
            cfg = load_config(catalog_path) if catalog_path else {}
            if collection:
                cfg = cfg.get("collections", {}).get(collection, cfg)
            if key in cfg:
                result[key] = {"value": cfg[key], "source": "config (INSECURE)"}

    return result


def get_setting_source(
    key: str,
    catalog_path: Path | None,
    collection: str | None,
) -> str:
    """Determine the source of a setting's value.

    Args:
        key: Setting key
        catalog_path: Path to catalog root
        collection: Optional collection name

    Returns:
        Source string: "env", "collection", "catalog", or "default"
    """
    # Check environment variable (skip empty strings)
    env_var = _get_env_var_name(key)
    env_value = os.environ.get(env_var)
    if env_value:  # Non-empty string
        return "env"

    # Check alias env vars (e.g., aws_profile aliases to profile, so check PORTOLAN_PROFILE)
    for alias in SETTING_ALIASES.get(key, []):
        alias_env_var = _get_env_var_name(alias)
        if os.environ.get(alias_env_var):
            return "env"

    if catalog_path is None:
        return "default"

    config = load_config(catalog_path)

    # Check collection config
    if collection is not None:
        collection_config = config.get("collections", {}).get(collection, {})
        if key in collection_config:
            return "collection"

    # Check catalog config
    if key in config:
        return "catalog"

    return "default"


# =============================================================================
# Hierarchical .portolan/ support (ADR-0039)
# =============================================================================


def find_portolan_files(
    start_path: Path,
    filename: str,
    catalog_root: Path,
) -> list[Path]:
    """Find all .portolan/{filename} from start_path up to catalog_root.

    Walks the directory tree from catalog_root to start_path, collecting
    paths to .portolan/{filename} files that exist. Returns them in order
    from catalog root to start_path (for merging: parent first, child last).

    Args:
        start_path: Directory to start from (collection or subcatalog).
        filename: File to look for (e.g., "config.yaml" or "metadata.yaml").
        catalog_root: Catalog root directory (stopping point).

    Returns:
        List of Paths in order from catalog root to start_path.
        Empty list if no .portolan/{filename} files exist.
    """
    # Resolve paths to handle symlinks and relative paths
    start_path = start_path.resolve()
    catalog_root = catalog_root.resolve()

    # Validate start_path is inside catalog_root (security: prevent path traversal)
    try:
        start_path.relative_to(catalog_root)
    except ValueError:
        # start_path is not inside catalog_root
        return []

    # Collect directories from start_path up to catalog_root (inclusive)
    directories: list[Path] = []
    current = start_path
    while True:
        directories.append(current)
        if current == catalog_root:
            break
        parent = current.parent
        # Safety: stop if we hit filesystem root (shouldn't happen after validation)
        if parent == current:
            break
        current = parent

    # Reverse to get catalog_root -> start_path order
    directories.reverse()

    # Find .portolan/{filename} in each directory
    result: list[Path] = []
    for directory in directories:
        portolan_dir = directory / ".portolan"
        file_path = portolan_dir / filename
        if file_path.is_file():
            result.append(file_path)

    return result


def _load_validated_mapping(file_path: Path) -> dict[str, Any] | None:
    """Load and validate a YAML file as a mapping.

    Handles YAML parsing errors and validates the document is a dict.
    Returns None for empty files or non-dict documents (with logging).

    Args:
        file_path: Path to the YAML file.

    Returns:
        Parsed dict if valid, None otherwise.

    Raises:
        ConfigInvalidStructureError: If YAML is malformed.
    """
    from portolan_cli.errors import ConfigInvalidStructureError

    content = file_path.read_text()
    if not content.strip():
        return None

    try:
        data = yaml.safe_load(content)
    except yaml.YAMLError as e:
        raise ConfigInvalidStructureError(str(file_path), f"Invalid YAML syntax: {e}") from e

    if not isinstance(data, dict):
        # Non-mapping YAML (e.g., a list or scalar) - skip with warning
        return None

    return data


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Deep merge two dictionaries with override taking precedence.

    For nested dictionaries, recursively merges. For other types,
    override value completely replaces base value.

    Args:
        base: Base dictionary (values may be overridden).
        override: Override dictionary (values take precedence).

    Returns:
        New merged dictionary.
    """
    result = dict(base)  # Shallow copy of base
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            # Recursively merge nested dicts
            result[key] = _deep_merge(result[key], value)
        else:
            # Override value completely
            result[key] = value
    return result


def load_merged_yaml(
    start_path: Path,
    filename: str,
    catalog_root: Path,
) -> dict[str, Any]:
    """Load and merge YAML files from .portolan/ hierarchy.

    Finds all .portolan/{filename} from catalog_root to start_path,
    then deep-merges them with child values overriding parent values.

    Args:
        start_path: Directory to start from (collection or subcatalog).
        filename: YAML file to look for (e.g., "config.yaml").
        catalog_root: Catalog root directory.

    Returns:
        Merged dictionary from all YAML files in hierarchy.
        Empty dict if no files exist.
    """
    files = find_portolan_files(start_path, filename, catalog_root)

    if not files:
        return {}

    result: dict[str, Any] = {}
    for file_path in files:
        data = _load_validated_mapping(file_path)
        if data is not None:
            result = _deep_merge(result, data)

    return result


def load_merged_config(
    path: Path,
    catalog_root: Path,
) -> dict[str, Any]:
    """Load merged config.yaml with backwards compatibility.

    Supports both:
    1. New: .portolan/config.yaml at each level (ADR-0039)
    2. Legacy: collections: section in root config.yaml (ADR-0024)

    Collection .portolan/config.yaml takes precedence over root collections:.

    Args:
        path: Path to collection or subcatalog directory.
        catalog_root: Catalog root directory.

    Returns:
        Merged configuration dictionary.
    """
    # Get the merged config from hierarchy
    merged = load_merged_yaml(path, CONFIG_FILENAME, catalog_root)

    # Check for legacy collections: section in root config
    root_config_path = catalog_root / ".portolan" / CONFIG_FILENAME
    if root_config_path.is_file():
        root_config = _load_validated_mapping(root_config_path)
        if root_config is not None:
            collections = root_config.get("collections", {})
            if isinstance(collections, dict):
                # Determine collection name from path
                try:
                    relative = path.resolve().relative_to(catalog_root.resolve())
                    collection_name = relative.parts[0] if relative.parts else None
                except ValueError:
                    collection_name = None

                if collection_name and collection_name in collections:
                    legacy_config = collections[collection_name]
                    if isinstance(legacy_config, dict):
                        # Merge: hierarchy config overrides legacy
                        # First apply legacy, then hierarchy on top
                        result: dict[str, Any] = {}
                        # Start with root-level settings (excluding collections:)
                        for key, value in root_config.items():
                            if key != "collections":
                                result[key] = value
                        # Apply legacy collection config
                        result = _deep_merge(result, legacy_config)
                        # Apply hierarchical folder configs (takes precedence)
                        # But we need to exclude the root config since we processed it
                        files = find_portolan_files(path, CONFIG_FILENAME, catalog_root)
                        for file_path in files:
                            if file_path != root_config_path:
                                data = _load_validated_mapping(file_path)
                                if data is not None:
                                    result = _deep_merge(result, data)
                        return result

    return merged


def load_merged_metadata(
    path: Path,
    catalog_root: Path,
) -> dict[str, Any]:
    """Load merged metadata.yaml from hierarchy.

    Simply delegates to load_merged_yaml for metadata.yaml files.

    Args:
        path: Path to collection or subcatalog directory.
        catalog_root: Catalog root directory.

    Returns:
        Merged metadata dictionary.
    """
    return load_merged_yaml(path, "metadata.yaml", catalog_root)


# =============================================================================
# .env file support (Issue #356)
# =============================================================================


def load_dotenv_from_catalog(catalog_path: Path | None = None) -> bool:
    """Load environment variables from .env file in catalog root.

    Searches for .env file in catalog root directory and loads it using
    python-dotenv. Does NOT override existing environment variables.

    This enables storing sensitive settings (remote, profile, region) in
    a .env file that won't be pushed to remote storage.

    Args:
        catalog_path: Path to catalog root. If None, attempts to find it.

    Returns:
        True if a .env file was found and loaded, False otherwise.
    """
    from dotenv import load_dotenv

    if catalog_path is None:
        return False

    env_file = catalog_path / ".env"
    if not env_file.is_file():
        return False

    load_dotenv(env_file, override=False)
    return True


def check_sensitive_settings_in_config(catalog_path: Path) -> list[str]:
    """Check if config.yaml contains sensitive settings that should be in .env.

    Args:
        catalog_path: Path to catalog root.

    Returns:
        List of sensitive setting names found in config.yaml.
    """
    config = load_config(catalog_path)
    found = []

    # Check top-level sensitive settings
    for key in SENSITIVE_SETTINGS:
        if key in config:
            found.append(key)

    # Check collection-level sensitive settings
    collections = config.get("collections", {})
    for collection_name, collection_conf in collections.items():
        if isinstance(collection_conf, dict):
            for key in SENSITIVE_SETTINGS:
                if key in collection_conf:
                    found.append(f"collections.{collection_name}.{key}")

    return found
