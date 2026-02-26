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

# Known settings for documentation/validation (but unknown keys are still allowed)
KNOWN_SETTINGS: frozenset[str] = frozenset({"remote", "aws_profile"})

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

    data = yaml.safe_load(content)
    return data if data is not None else {}


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

    Args:
        key: Setting key (e.g., "aws_profile")

    Returns:
        Environment variable name (e.g., "PORTOLAN_AWS_PROFILE")
    """
    return f"PORTOLAN_{key.upper()}"


def get_setting(
    key: str,
    cli_value: Any | None = None,
    catalog_path: Path | None = None,
    collection: str | None = None,
) -> Any | None:
    """Resolve a setting with full precedence.

    Precedence (highest to lowest):
    1. CLI argument (cli_value)
    2. Environment variable (PORTOLAN_<KEY>)
    3. Collection-level config (if collection specified)
    4. Catalog-level config
    5. Built-in default (None)

    Args:
        key: Setting key (e.g., "remote", "aws_profile")
        cli_value: Value passed via CLI argument (highest precedence)
        catalog_path: Path to catalog root for loading config file
        collection: Optional collection name for collection-level config

    Returns:
        Resolved value, or None if not found at any level.
    """
    # 1. CLI argument takes highest precedence
    if cli_value is not None:
        return cli_value

    # 2. Environment variable
    env_var = _get_env_var_name(key)
    env_value = os.environ.get(env_var)
    if env_value is not None:
        return env_value

    # If no catalog path, can't check file-based config
    if catalog_path is None:
        return None

    # Load config from file
    config = load_config(catalog_path)

    # 3. Collection-level config (if collection specified)
    if collection is not None:
        collections = config.get("collections", {})
        collection_config = collections.get(collection, {})
        if key in collection_config:
            return collection_config[key]

    # 4. Catalog-level config
    if key in config:
        return config[key]

    # 5. Default (None)
    return None


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
        key: Setting key (e.g., "remote", "aws_profile")
        value: Value to set
        collection: Optional collection name for collection-level config
    """
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

    # Check environment variables for all known settings
    for key in KNOWN_SETTINGS:
        env_var = _get_env_var_name(key)
        if env_var in os.environ:
            all_keys.add(key)

    # Resolve each setting
    for key in sorted(all_keys):
        value = get_setting(key, catalog_path=catalog_path, collection=collection)
        source = _get_setting_source(key, catalog_path, collection)
        if value is not None or source != "default":
            result[key] = {"value": value, "source": source}

    return result


def _get_setting_source(
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
    # Check environment variable
    env_var = _get_env_var_name(key)
    if env_var in os.environ:
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
