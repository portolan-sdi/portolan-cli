"""Configuration management for Portolan catalogs.

Precedence, highest first: CLI argument, environment, collection config, catalog config, default.
"""

from __future__ import annotations

import contextlib
import copy
import logging
import os
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

import yaml

from portolan_core.errors import ConfigError, ConfigInvalidStructureError, ConfigParseError
from portolan_core.trace import count

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable
    from pathlib import Path
    from typing import TypeAlias

    #: Reads and parses one YAML file. ``parse_yaml``, or the memo of a run state.
    YamlReader: TypeAlias = Callable[[Path], Any]

logger = logging.getLogger(__name__)


class ConfigSource(Protocol):
    """The config methods of ``run_state.RunState``, as a reader types them.

    Readers do not import ``run_state``, which reaches ``sync``; import-linter bans that path.
    """

    catalog_root: Path

    def config(self, directory: Path) -> dict[str, Any]:
        """The merged config.yaml hierarchy that applies to ``directory``."""
        ...

    def metadata(self, directory: Path, *, inherit: bool = True) -> dict[str, Any]:
        """The merged metadata.yaml hierarchy that applies to ``directory``."""
        ...

    def setting(
        self, key: str, *, collection_dir: Path | None = None, cli_value: Any | None = None
    ) -> Any | None:
        """A setting with the precedence of ``get_setting``."""
        ...


# Never stored in config.yaml: it is pushed to remote storage and would expose credentials.
SENSITIVE_SETTINGS: frozenset[str] = frozenset(
    {
        "remote",
        "aws_profile",
        "profile",
        "region",
        "s3_endpoint",
        "s3_use_ssl",
    }
)

#: Every setting ``config set`` accepts, with its type; ``.*`` takes any name below the prefix.
#: Reads stay lenient, so a plugin can read its own hand-written key through ``get_setting``.
KNOWN_SETTINGS: dict[str, str] = {
    "backend": "str",
    "ignored_files": "list",
    "statistics.enabled": "bool",
    "statistics.raster_mode": "str",
    "parquet.enabled": "bool",  # The items.parquet item mirror (opt-out)
    "partitioning.enabled": "bool",  # Auto-partition large GeoParquet files
    "partitioning.prompt": "bool",
    "partitioning.threshold_gb": "number",
    "partitioning.strategy": "str",
    "partitioning.target_rows": "int",
    "pmtiles.enabled": "bool",
    "pmtiles.min_zoom": "int",
    "pmtiles.max_zoom": "int",
    "pmtiles.layer": "str",
    "pmtiles.bbox": "str",  # "minx,miny,maxx,maxy"
    "pmtiles.where": "str",
    "pmtiles.include_cols": "str",  # Comma-separated column names
    "pmtiles.precision": "int",
    "pmtiles.attribution": "str",
    "pmtiles.src_crs": "str",
    "push.exclude": "list",  # Metadata sync exclusions
    "tabular.enabled": "bool",  # Non-geo tabular assets
    "tabular.convert": "bool",
    "conversion.extensions.convert": "list",
    "conversion.extensions.preserve": "list",
    "conversion.paths.preserve": "list",
    "conversion.handlers.*": "str",  # Format handler pin per extension
    "conversion.cog.compression": "str",
    "conversion.cog.quality": "int",
    "conversion.cog.tile_size": "int",
    "conversion.cog.predictor": "int|auto",
    "conversion.cog.resampling": "str",
    "conversion.cog.generate_thumbnail": "bool",
    "conversion.cog.thumbnail_max_size": "int",
    "conversion.cog.thumbnail_quality": "int",
    "conversion.vector.spatial_index": "str",
    "conversion.vector.resolution": "int|auto",
    "conversion.vector.sort": "str",
    "conversion.vector.add_bbox": "bool",
    "conversion.vector.partition": "bool",
    "styles.vector.point.circle-color": "str",
    "styles.vector.point.circle-radius": "int",
    "styles.vector.point.circle-opacity": "number",
    "styles.vector.line.line-color": "str",
    "styles.vector.line.line-width": "int",
    "styles.vector.line.line-opacity": "number",
    "styles.vector.polygon.fill-color": "str",
    "styles.vector.polygon.fill-opacity": "number",
    "styles.vector.polygon.fill-outline-color": "str",
    "styles.raster.colormap": "str",
    "styles.raster.rescale": "list",
    "thumbnails.enabled": "bool",
    "thumbnails.max_size": "int",
    "thumbnails.quality": "int",
    "thumbnails.max_features": "int",
    "thumbnails.basemap.provider": "str",
    "thumbnails.basemap.opacity": "number",
    "thumbnails.basemap.zoom_adjust": "int",
    "thumbnails.basemap.api_key": "str",
    "check.disabled": "list",
    "check.severity.*": "str",
    "publish.public_url": "str",
}

#: The settings that a reader resolves per collection, as key prefixes. Any
#: other reader reads the catalog level only, so ``set_setting`` rejects a
#: collection value for its key.
_COLLECTION_SETTINGS: tuple[str, ...] = (
    "backend",
    "parquet.enabled",
    "partitioning.enabled",
    "partitioning.strategy",
    "partitioning.target_rows",
    "partitioning.threshold_gb",
    "pmtiles.",
    "statistics.",
    "tabular.",
)


def _applies_per_collection(key: str) -> bool:
    """Return True when a reader resolves ``key`` per collection."""
    return key.startswith(_COLLECTION_SETTINGS)


#: How an error message names each type in ``KNOWN_SETTINGS``.
_TYPE_LABELS: dict[str, str] = {
    "bool": "true or false",
    "int": "an integer",
    "number": "a number",
    "int|auto": "an integer or 'auto'",
    "str": "a string",
    "list": "a comma-separated list",
}

# Setting aliases: maps canonical key -> list of alternative keys to check
# When looking up aws_profile, also check for "profile" in the config
SETTING_ALIASES: dict[str, list[str]] = {
    "aws_profile": ["profile"],
}

# Default values for settings ( for statistics)
DEFAULT_SETTINGS: dict[str, Any] = {
    "statistics.enabled": True,
    "statistics.raster_mode": "approx",
    # The spec says the item mirror SHOULD be published, with no item-count threshold.
    "parquet.enabled": True,
    "partitioning.enabled": True,  # Auto-partition large GeoParquet files (>2GB)
    "partitioning.prompt": True,  # Prompt before partitioning in interactive mode
    "partitioning.threshold_gb": 2.0,  # 2GB per OGC best practices
    "partitioning.strategy": "kdtree",  # KD-tree: data-driven, auto-balancing
    "partitioning.target_rows": 120_000,  # geoparquet-io default
    "pmtiles.enabled": False,  # Disabled by default (requires tippecanoe)
    "pmtiles.min_zoom": None,  # None = tippecanoe auto-detection
    "pmtiles.max_zoom": None,  # None = tippecanoe auto-detection
    "pmtiles.layer": None,  # None = use output filename
    "pmtiles.bbox": None,  # None = no bounding box filter
    "pmtiles.where": None,  # None = no SQL filter
    "pmtiles.include_cols": None,  # None = include all columns
    "pmtiles.precision": 6,  # Coordinate decimal precision
    "pmtiles.attribution": None,  # None = geoparquet-io default
    "pmtiles.src_crs": None,  # None = use metadata CRS
    # Security: push.py's _SECURITY_EXCLUDE_PATTERNS also enforces .env, .git/, .portolan/.
    # A pattern that ends in / matches when ANY path component equals the name.
    "push.exclude": [
        ".portolan/",  # Internal Portolan state
        ".git/",  # Git repository data
        ".env",  # Environment files with secrets
        ".env.*",  # Environment file variants (.env.local, etc.)
        "*.py",  # Python source files
        "*.pyc",  # Python bytecode
        "__pycache__/",  # Python cache directories
        ".DS_Store",  # macOS metadata
        "Thumbs.db",  # Windows thumbnails
        "*.log",  # Log files
        "*.tmp",  # Temporary files
        "*.bak",  # Backup files
        "*~",  # Editor backup files
    ],
    "tabular.enabled": False,  # Disabled by default; opt-in per ADR scope
    "tabular.convert": True,  # Convert CSV/TSV/Excel to Parquet by default
}

# Default glob patterns for files to exclude from asset tracking; config.yaml can replace them.
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
    ".env",  # Environment files with credentials
    ".env.*",  # .env.local, .env.production, etc.
    ".env.local",  # Explicit for common pattern
]


@dataclass(frozen=True)
class S3EndpointSettings:
    """S3-compatible endpoint settings resolved from the process environment.

    Security: they stay out of config.yaml, which is published and would reveal storage topology.
    """

    endpoint: str | None
    use_ssl: bool


def resolve_s3_endpoint_settings() -> S3EndpointSettings:
    """Resolve S3-compatible endpoint settings from the process environment.

    Security: only ``true`` or ``false`` is accepted, so a typo cannot turn TLS off.
    """
    endpoint = os.environ.get("PORTOLAN_S3_ENDPOINT") or None
    raw_use_ssl = os.environ.get("PORTOLAN_S3_USE_SSL")
    if raw_use_ssl is None:
        return S3EndpointSettings(endpoint=endpoint, use_ssl=True)

    normalized = raw_use_ssl.strip().lower()
    if normalized == "true":
        return S3EndpointSettings(endpoint=endpoint, use_ssl=True)
    if normalized == "false":
        return S3EndpointSettings(endpoint=endpoint, use_ssl=False)
    raise ValueError("PORTOLAN_S3_USE_SSL must be 'true' or 'false'")


# Config file name (inside .portolan/)
CONFIG_FILENAME = "config.yaml"
METADATA_FILENAME = "metadata.yaml"


def get_config_path(catalog_path: Path) -> Path:
    """Get the path to the config file for a catalog."""
    return catalog_path / ".portolan" / CONFIG_FILENAME


def safe_load(text: str) -> Any:
    """Parse YAML text with the safe loader, through libyaml when PyYAML has it."""
    loader: Any = getattr(yaml, "CSafeLoader", yaml.SafeLoader)
    # Count at the shared parser: callers can parse text without parse_yaml.
    count("config.yaml_parses")
    return yaml.load(text, Loader=loader)  # noqa: S506 (a safe loader, chosen above)


def parse_yaml(file_path: Path) -> Any:
    """Read and parse a config.yaml or metadata.yaml file."""
    content = file_path.read_text(encoding="utf-8")
    data: Any = None
    if content.strip():
        data = safe_load(content)
    if isinstance(data, dict) and file_path.name == CONFIG_FILENAME:
        data = _fold_config(data)
    return data


def _schema_entry(key: str) -> tuple[tuple[str, ...], str] | None:
    """Look a dotted key up in ``KNOWN_SETTINGS``."""
    if key.endswith(".*"):
        return None
    kind = KNOWN_SETTINGS.get(key)
    if kind is not None:
        return tuple(key.split(".")), kind
    for pattern, pattern_kind in KNOWN_SETTINGS.items():
        prefix = pattern.removesuffix("*")
        if prefix != pattern and key.startswith(prefix) and len(key) > len(prefix):
            return (*prefix[:-1].split("."), key[len(prefix) :]), pattern_kind
    return None


def _matches(kind: str, value: Any) -> bool:
    """Return True when ``value`` has the type that ``kind`` names."""
    is_int = isinstance(value, int) and not isinstance(value, bool)
    checks = {
        "bool": isinstance(value, bool),
        "int": is_int,
        "number": is_int or isinstance(value, float),
        "int|auto": is_int or value == "auto",
        "str": isinstance(value, str),
        # A date or a nested value has no reader, and JSON output cannot hold a date.
        "list": isinstance(value, list)
        and all(isinstance(item, (str, int, float)) for item in value),
    }
    return checks[kind]


def _parse_text(kind: str, text: str) -> Any:
    """Turn the text of a ``config set`` VALUE into the type that ``kind`` names."""
    if kind == "str":
        return text
    if kind == "list" and not text.lstrip().startswith("["):
        return [item.strip() for item in text.split(",") if item.strip()]
    try:
        parsed = safe_load(text)
    except yaml.YAMLError:
        return text
    if kind == "number" and isinstance(parsed, str) and _FLOAT_TEXT.fullmatch(parsed):
        # YAML 1.1 reads 1e-3 and -.5 as text. A number setting takes them.
        return float(parsed)
    return parsed


#: A decimal number with an optional exponent, such as 1e-3, -.5, or 2.0.
_FLOAT_TEXT = re.compile(r"[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?")


def _coerce_value(key: str, kind: str, value: Any) -> Any:
    """Give a setting value the type that ``KNOWN_SETTINGS`` names for it."""
    parsed = _parse_text(kind, value) if isinstance(value, str) else value
    if not _matches(kind, parsed):
        raise ConfigError(f"Setting '{key}' takes {_TYPE_LABELS[kind]}, got {value!r}")
    return parsed


def _set_path(target: dict[str, Any], path: tuple[str, ...], value: Any) -> None:
    """Write ``value`` at a nested path. A missing or non-mapping level becomes a mapping."""
    node = target
    for part in path[:-1]:
        child = node.get(part)
        if not isinstance(child, dict):
            child = {}
            node[part] = child
        node = child
    node[path[-1]] = value


def _pop_path(target: dict[str, Any], path: tuple[str, ...]) -> bool:
    """Remove the value at a nested path and every mapping that it leaves empty."""
    parents: list[tuple[dict[str, Any], str]] = []
    node = target
    for part in path[:-1]:
        child = node.get(part)
        if not isinstance(child, dict):
            return False
        parents.append((node, part))
        node = child
    if path[-1] not in node:
        return False
    del node[path[-1]]
    for parent, part in reversed(parents):
        if parent[part]:
            break
        del parent[part]
    return True


def _fold_dotted_keys(mapping: dict[Any, Any]) -> dict[str, Any]:
    """Move legacy flat dotted keys that ``config set`` wrote into nested form.

    Such a key wins over the nested value, and its value gets the schema type when it can.
    """
    flat = {
        key: entry
        for key in mapping
        if isinstance(key, str) and "." in key and (entry := _schema_entry(key)) is not None
    }
    if not flat:
        return mapping
    folded = {key: value for key, value in mapping.items() if key not in flat}
    for key, (path, kind) in flat.items():
        value = mapping[key]
        # A bad value stays as it is. The reader warns, as for a nested value.
        with contextlib.suppress(ConfigError):
            value = _coerce_value(key, kind, value)
        _set_path(folded, path, value)
    return folded


def _fold_config(data: dict[str, Any]) -> dict[str, Any]:
    """Fold legacy dotted keys at the catalog level and in each ``collections`` entry."""
    data = _fold_dotted_keys(data)
    collections = data.get("collections")
    if isinstance(collections, dict):
        data["collections"] = {
            name: _fold_dotted_keys(entry) if isinstance(entry, dict) else entry
            for name, entry in collections.items()
        }
    return data


@dataclass(frozen=True)
class CatalogConfig:
    """The ``.portolan/config.yaml`` of one catalog.

    ``data`` can be a parse a run state shares, so never mutate it; ``load_config`` copies.
    """

    data: dict[str, Any]

    def section(self, *path: str) -> dict[str, Any]:
        """Return the mapping at a nested path, or an empty dict."""
        node: Any = self.data
        for part in path:
            node = node.get(part) if isinstance(node, dict) else None
        return node if isinstance(node, dict) else {}


def load_catalog_config(catalog_path: Path, *, state: ConfigSource | None = None) -> CatalogConfig:
    """Load the catalog-level config that every reader in one run shares."""
    if state is not None:
        return CatalogConfig(state.config(catalog_path))
    return CatalogConfig(with_env(catalog_file_config(catalog_path).data))


def catalog_file_config(catalog_path: Path, read: YamlReader = parse_yaml) -> CatalogConfig:
    """Load ``.portolan/config.yaml`` without the environment."""
    config_file = get_config_path(catalog_path)
    if not config_file.is_file():
        return CatalogConfig({})
    try:
        data = read(config_file)
    except yaml.YAMLError as e:
        raise ConfigParseError(str(config_file), str(e)) from e
    if data is None:
        return CatalogConfig({})
    if not isinstance(data, dict):
        raise ConfigInvalidStructureError(str(config_file), "Config must be a YAML mapping")
    if "collections" in data and not isinstance(data["collections"], dict):
        raise ConfigInvalidStructureError(str(config_file), "'collections' must be a mapping")
    return CatalogConfig(data)


def load_config(catalog_path: Path) -> dict[str, Any]:
    """Load configuration from .portolan/config.yaml."""
    return copy.deepcopy(catalog_file_config(catalog_path).data)


def get_ignored_files(catalog_path: Path | None, *, state: ConfigSource | None = None) -> list[str]:
    """Return the list of glob patterns for files to exclude from asset tracking.

    A config value replaces the defaults entirely, as a ``.gitignore`` at a scope does.
    """
    if catalog_path is None:
        return list(DEFAULT_IGNORED_FILES)

    config = load_catalog_config(catalog_path, state=state).data

    if "ignored_files" not in config:
        return list(DEFAULT_IGNORED_FILES)

    raw = config["ignored_files"]

    if not isinstance(raw, list):
        raise ConfigInvalidStructureError(
            str(get_config_path(catalog_path)),
            "'ignored_files' must be a list of glob patterns (strings)",
        )

    for item in raw:
        if not isinstance(item, str):
            raise ConfigInvalidStructureError(
                str(get_config_path(catalog_path)),
                f"'ignored_files' must be a list of strings; got item {item!r}",
            )

    return list(raw)


def save_config(catalog_path: Path, config: dict[str, Any]) -> None:
    """Save configuration to .portolan/config.yaml."""
    portolan_dir = catalog_path / ".portolan"
    portolan_dir.mkdir(parents=True, exist_ok=True)

    config_file = portolan_dir / CONFIG_FILENAME

    # Use default_flow_style=False for readable multi-line YAML
    content = yaml.safe_dump(config, default_flow_style=False, sort_keys=False)
    config_file.write_text(content, encoding="utf-8")


def _get_env_var_name(key: str) -> str:
    """Convert a setting key to environment variable name."""
    import re

    # Replace non-alphanumeric chars with underscores, then uppercase
    normalized = re.sub(r"[^A-Za-z0-9]", "_", key)
    return f"PORTOLAN_{normalized.upper()}"


def _env_setting(key: str) -> str | None:
    """Read a setting from the environment, through the key or an alias."""
    env_value = os.environ.get(_get_env_var_name(key))
    if env_value:
        return env_value
    # An alias gives a second name. "aws_profile" aliases to "profile", so
    # PORTOLAN_PROFILE also sets it.
    for alias in SETTING_ALIASES.get(key, []):
        alias_value = os.environ.get(_get_env_var_name(alias))
        if alias_value:
            return alias_value
    return None


#: Environment variable name to the dotted key that it sets, for each fixed key.
_ENV_KEYS: dict[str, str] = {
    _get_env_var_name(key): key for key in KNOWN_SETTINGS if not key.endswith(".*")
}

#: How each ``.*`` pattern turns the rest of a variable name back into a name.
#: A variable name holds no case and no punctuation, so each pattern restores
#: the form of its names. An extension is lower case with dots, and a rule id
#: keeps upper case with hyphens. The table must hold every pattern in
#: ``KNOWN_SETTINGS``, or the import fails.
_ENV_PATTERN_NAMES: dict[str, Callable[[str], str]] = {
    "conversion.handlers.*": lambda rest: rest.lower().replace("_", "."),
    "check.severity.*": lambda rest: rest.replace("_", "-"),
}

#: Variable name prefix to its pattern and name rule, such as
#: "PORTOLAN_CHECK_SEVERITY_" for "check.severity.*".
_ENV_PATTERNS: dict[str, tuple[str, Callable[[str], str]]] = {
    _get_env_var_name(pattern[:-1]): (pattern[:-1], _ENV_PATTERN_NAMES[pattern])
    for pattern in KNOWN_SETTINGS
    if pattern.endswith(".*")
}


def _env_key(name: str) -> str | None:
    """Return the dotted key that an environment variable sets, or None."""
    key = _ENV_KEYS.get(name)
    if key is not None:
        return key
    for prefix, (key_prefix, restore) in _ENV_PATTERNS.items():
        if name.startswith(prefix) and len(name) > len(prefix):
            return key_prefix + restore(name[len(prefix) :])
    return None


def _env_overrides() -> dict[str, Any]:
    """Collect the ``PORTOLAN_<KEY>`` variables of known settings as a nested config."""
    overrides: dict[str, Any] = {}
    for name, text in os.environ.items():
        key = _env_key(name) if text and name.startswith("PORTOLAN_") else None
        entry = _schema_entry(key) if key is not None else None
        if key is None or entry is None:
            continue
        value: Any = text
        with contextlib.suppress(ConfigError):
            value = _coerce_value(key, entry[1], text)
        _set_path(overrides, entry[0], value)
    return overrides


def _key_path(key: str) -> tuple[str, ...]:
    """Split a dotted key into its nested path, as ``KNOWN_SETTINGS`` defines it."""
    entry = _schema_entry(key)
    return entry[0] if entry is not None else tuple(key.split("."))


def _get_nested(cfg: dict[str, Any], key: str) -> Any | None:
    """Walk a nested config dict for a dotted key such as "pmtiles.src_crs"."""
    current: Any = cfg
    for part in _key_path(key):
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]
    return current


def _get_with_aliases(cfg: dict[str, Any], key: str) -> Any | None:
    """Read a key from a config dict, through the key, a dotted path, or an alias."""
    if key in cfg:
        return cfg[key]
    if "." in key:
        nested_val = _get_nested(cfg, key)
        if nested_val is not None:
            return nested_val
    for alias in SETTING_ALIASES.get(key, []):
        if alias in cfg:
            return cfg[alias]
    return None


def _check_sensitive_in_config(cfg: dict[str, Any], key: str) -> None:
    """Reject a sensitive setting that a config file holds."""
    if key in SENSITIVE_SETTINGS and (
        key in cfg or any(a in cfg for a in SETTING_ALIASES.get(key, []))
    ):
        env_var_name = _get_env_var_name(key)
        raise ValueError(
            f"'{key}' found in config.yaml but sensitive settings cannot be read from "
            f"config files. Use environment variable {env_var_name} or .env file."
        )


def get_setting(
    key: str,
    cli_value: Any | None = None,
    catalog_path: Path | None = None,
    collection: str | None = None,
    collection_path: Path | None = None,
    *,
    state: ConfigSource | None = None,
) -> Any | None:
    """Resolve a setting with full precedence.

    Order: CLI value, environment, collection config, ``collections:`` section, catalog, default.

    Args:
        key: Setting key (e.g., "remote", "aws_profile").
        cli_value: Value passed via CLI argument (highest precedence).
        catalog_path: Path to catalog root for loading config file.
        collection: Optional collection name for legacy collection-level config.
        collection_path: Optional path to collection directory for hierarchical lookup.
        state: The run state, which shares file reads across settings.

    Returns:
        Resolved value, or None if not found; a list or mapping is a copy the caller may edit.
    """
    if catalog_path is None:
        return resolve_setting(key, cli_value, None)
    root = catalog_path

    def layers() -> Iterable[dict[str, Any]]:
        """Return the config mappings to read, the first one wins."""
        # 3. The hierarchical .portolan/ config of the collection directory.
        if collection_path is not None:
            if state is not None:
                return (state.config(collection_path),)
            return (with_env(merge_config_files(collection_path, root)),)
        config = (
            load_catalog_config(root, state=state)
            if state is not None
            else catalog_file_config(root)
        )
        # 4. The legacy collections: section, then 5. the catalog level.
        if collection is not None:
            return (config.section("collections", collection), config.data)
        return (config.data,)

    return resolve_setting(key, cli_value, layers)


def resolve_setting(
    key: str,
    cli_value: Any | None,
    layers: Callable[[], Iterable[dict[str, Any]]] | None,
) -> Any | None:
    """Resolve a setting in the order of ``get_setting``."""
    # 1. CLI argument takes highest precedence
    if cli_value is not None:
        return cli_value

    # 2. Environment variable (skip empty strings)
    env_value = _env_setting(key)
    if env_value is not None:
        # The value takes the schema type, as a config set value does.
        entry = _schema_entry(key)
        with contextlib.suppress(ConfigError):
            return _coerce_value(key, entry[1], env_value) if entry else env_value
        return env_value

    if layers is None:
        return None
    for layer in layers():
        _check_sensitive_in_config(layer, key)
        value = _get_with_aliases(layer, key)
        if value is not None:
            return copy.deepcopy(value)

    # 6. Built-in default from DEFAULT_SETTINGS
    return copy.deepcopy(DEFAULT_SETTINGS.get(key))


def _unknown_setting_message(key: str) -> str:
    names = ", ".join(sorted(name.replace("*", "<name>") for name in KNOWN_SETTINGS))
    return f"Unknown setting '{key}'. Known settings: {names}"


def set_setting(
    catalog_path: Path,
    key: str,
    value: Any,
    collection: str | None = None,
) -> Any:
    """Set a configuration value."""
    if key in SENSITIVE_SETTINGS:
        env_var = _get_env_var_name(key)
        raise ConfigError(
            f"'{key}' cannot be stored in config.yaml (would be pushed to remote). "
            f"Use environment variable {env_var} or add to .env file in catalog root."
        )

    entry = _schema_entry(key)
    if entry is None:
        raise ConfigError(_unknown_setting_message(key))
    path, kind = entry
    if collection is not None and not _applies_per_collection(key):
        raise ConfigError(
            f"Setting '{key}' applies to the whole catalog. "
            "Run the command again without --collection."
        )
    value = _coerce_value(key, kind, value)

    config = load_config(catalog_path)
    scope = ("collections", collection) if collection is not None else ()
    _set_path(config, (*scope, *path), value)
    save_config(catalog_path, config)
    return value


def unset_setting(
    catalog_path: Path,
    key: str,
    collection: str | None = None,
) -> bool:
    """Remove a configuration value."""
    config = load_config(catalog_path)
    scope = ("collections", collection) if collection is not None else ()
    if not (_pop_path(config, (*scope, key)) or _pop_path(config, (*scope, *_key_path(key)))):
        return False
    save_config(catalog_path, config)
    return True


def _leaf_keys(mapping: dict[str, Any], prefix: str = "") -> list[str]:
    """List the dotted keys of the values in a nested config mapping."""
    keys: list[str] = []
    for name, value in mapping.items():
        dotted = f"{prefix}{name}"
        if isinstance(value, dict) and value and f"{dotted}.*" not in KNOWN_SETTINGS:
            keys.extend(_leaf_keys(value, f"{dotted}."))
        else:
            keys.append(dotted)
    return keys


def list_settings(
    catalog_path: Path | None = None,
    collection: str | None = None,
    *,
    state: ConfigSource | None = None,
) -> dict[str, dict[str, Any]]:
    """List all settings with their sources."""
    result: dict[str, dict[str, Any]] = {}

    config = CatalogConfig({})
    if catalog_path is not None:
        config = (
            load_catalog_config(catalog_path, state=state)
            if state is not None
            else catalog_file_config(catalog_path)
        )
    file_config = {key: value for key, value in config.data.items() if key != "collections"}
    all_keys = set(_leaf_keys(file_config))

    collection_config = config.section("collections", collection) if collection else {}
    all_keys.update(_leaf_keys(collection_config))

    # Add known settings. A ``.*`` pattern resolves to nothing and drops out.
    all_keys.update(KNOWN_SETTINGS)

    # Check environment variables for all known + sensitive settings
    # Sensitive settings can still be read from env vars, just not saved to config
    for key in KNOWN_SETTINGS.keys() | SENSITIVE_SETTINGS:
        env_var = _get_env_var_name(key)
        if env_var in os.environ:
            all_keys.add(key)

    # Resolve each setting
    for key in sorted(all_keys):
        try:
            value = get_setting(key, catalog_path=catalog_path, collection=collection, state=state)
            source = get_setting_source(key, catalog_path, collection, state=state)
            if value is not None:
                result[key] = {"value": value, "source": source}
        except ValueError:
            # Sensitive key in config without env var - show with warning source
            cfg = collection_config or config.data
            if key in cfg:
                result[key] = {"value": cfg[key], "source": "config (INSECURE)"}

    return result


def get_setting_source(
    key: str,
    catalog_path: Path | None,
    collection: str | None,
    *,
    state: ConfigSource | None = None,
) -> str:
    """Determine the source of a setting's value."""
    if _env_setting(key) is not None:
        return "env"

    if catalog_path is None:
        return "default"

    config = (
        load_catalog_config(catalog_path, state=state)
        if state is not None
        else catalog_file_config(catalog_path)
    )

    # Check collection config
    if (
        collection is not None
        and _get_with_aliases(config.section("collections", collection), key) is not None
    ):
        return "collection"

    # Check catalog config
    if _get_with_aliases(config.data, key) is not None:
        return "catalog"

    return "default"


@dataclass(frozen=True)
class SettingValue:
    """One setting, as ``config get`` and ``config set`` report it."""

    key: str
    value: Any
    source: str | None = None
    collection: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Serialize the setting as the envelope data of its command."""
        data: dict[str, Any] = {"key": self.key, "value": self.value}
        if self.source is not None:
            data["source"] = self.source
        if self.collection:
            data["collection"] = self.collection
        return data


def read_setting(
    key: str,
    catalog_path: Path,
    collection: str | None = None,
    *,
    state: ConfigSource | None = None,
) -> SettingValue:
    """Resolve one setting and its source."""
    value = get_setting(key, catalog_path=catalog_path, collection=collection, state=state)
    source = get_setting_source(key, catalog_path, collection, state=state)
    return SettingValue(key, value, source, collection)


def store_setting(
    catalog_path: Path,
    key: str,
    value: Any,
    collection: str | None = None,
) -> SettingValue:
    """Set a configuration value and report what the file stores."""
    return SettingValue(key, set_setting(catalog_path, key, value, collection), None, collection)


@dataclass(frozen=True)
class SettingList:
    """Each setting that ``config list`` reports, with its value and source."""

    settings: dict[str, dict[str, Any]]
    collection: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Serialize the settings as the ``config list`` envelope data."""
        data: dict[str, Any] = {"settings": self.settings}
        if self.collection:
            data["collection"] = self.collection
        return data


def read_settings(
    catalog_path: Path,
    collection: str | None = None,
    *,
    state: ConfigSource | None = None,
) -> SettingList:
    """List each setting with its value and source."""
    return SettingList(list_settings(catalog_path, collection, state=state), collection)


@dataclass(frozen=True)
class SettingRemoval:
    """The outcome of ``config unset``: the key, and whether config.yaml held it."""

    key: str
    removed: bool
    collection: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Serialize the outcome as the ``config unset`` envelope data."""
        data: dict[str, Any] = {"key": self.key, "removed": self.removed}
        if self.collection:
            data["collection"] = self.collection
        return data


def clear_setting(
    catalog_path: Path,
    key: str,
    collection: str | None = None,
) -> SettingRemoval:
    """Remove a configuration value from config.yaml."""
    return SettingRemoval(key, unset_setting(catalog_path, key, collection), collection)


# Hierarchical .portolan/ support


def find_portolan_files(
    start_path: Path,
    filename: str,
    catalog_root: Path,
) -> list[Path]:
    """Find all .portolan/{filename} from start_path up to catalog_root.

    The list runs from catalog root to start_path, so a merge puts the child last.
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


def _load_validated_mapping(file_path: Path, read: YamlReader) -> dict[str, Any] | None:
    """Load a YAML file as a mapping."""
    try:
        data = read(file_path)
    except yaml.YAMLError as e:
        raise ConfigInvalidStructureError(str(file_path), f"Invalid YAML syntax: {e}") from e
    return data if isinstance(data, dict) else None


def _merge_files(files: list[Path], read: YamlReader, skip: Path | None = None) -> dict[str, Any]:
    """Deep-merge the mappings in ``files``, later files over earlier ones.

    The result shares nested values with the parses; copy it before it leaves this module.
    """
    result: dict[str, Any] = {}
    for file_path in files:
        if file_path == skip:
            continue
        data = _load_validated_mapping(file_path, read)
        if data is not None:
            result = _deep_merge(result, data)
    return result


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Deep merge two dictionaries with override taking precedence."""
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
    """Load and merge YAML files from .portolan/ hierarchy."""
    count("config.merges")
    files = find_portolan_files(start_path, filename, catalog_root)
    return copy.deepcopy(_merge_files(files, parse_yaml))


#: The (file, key) pairs that ``_warn_catalog_only_keys`` reported in this process.
_WARNED: set[tuple[Path, str]] = set()


def _warn_catalog_only_keys(file_path: Path, mapping: dict[str, Any], prefix: str = "") -> None:
    """Warn once per file and key about a catalog-only key in a collection config."""
    for key in _leaf_keys(mapping):
        known = _schema_entry(key) is not None or f"{key}.*" in KNOWN_SETTINGS
        name = prefix + key
        if not known or _applies_per_collection(key) or (file_path, name) in _WARNED:
            continue
        _WARNED.add((file_path, name))
        logger.warning(
            "%s: '%s' applies to the whole catalog, so no reader sees it here. "
            "Move it to the catalog config.yaml.",
            file_path,
            name,
        )


def load_merged_config(
    path: Path,
    catalog_root: Path,
) -> dict[str, Any]:
    """Load merged config.yaml with backwards compatibility."""
    return copy.deepcopy(with_env(merge_config_files(path, catalog_root)))


def with_env(mapping: dict[str, Any]) -> dict[str, Any]:
    """Put the ``PORTOLAN_<KEY>`` variables of known settings on top of a config."""
    overrides = _env_overrides()
    return _deep_merge(mapping, overrides) if overrides else mapping


def merge_config_files(
    path: Path, catalog_root: Path, read: YamlReader = parse_yaml
) -> dict[str, Any]:
    """Merge the config.yaml files that apply to ``path``, without the environment.

    The result shares nested values with the parses, so do not mutate it.
    """
    count("config.merges")
    root_config = catalog_file_config(catalog_root, read)
    files = find_portolan_files(path, CONFIG_FILENAME, catalog_root)
    root_file = get_config_path(catalog_root).resolve()
    for file_path in files:
        if file_path != root_file:
            _warn_catalog_only_keys(file_path, _load_validated_mapping(file_path, read) or {})
    merged = _merge_files(files, read)

    # Legacy collections: section in the root config. The hierarchy files
    # below the root override it, and it overrides the root settings.
    try:
        parts = path.resolve().relative_to(catalog_root.resolve()).parts
    except ValueError:
        parts = ()
    # A nested collection id such as "parent/child" is one key. The longest
    # leading part of the path that names a section wins.
    sections = root_config.section("collections")
    names = ("/".join(parts[:end]) for end in range(len(parts), 0, -1))
    name = next((name for name in names if isinstance(sections.get(name), dict)), None)
    if name is not None:
        legacy = sections[name]
        _warn_catalog_only_keys(root_file, legacy, f"collections.{name}.")
        root_settings = {k: v for k, v in root_config.data.items() if k != "collections"}
        merged = _deep_merge(
            _deep_merge(root_settings, legacy), _merge_files(files, read, skip=root_file)
        )
    return merged


#: metadata.yaml keys that name one object and therefore never inherit.
#: Every other key, such as the license or the contact, describes a whole subtree.
NON_INHERITED_METADATA_KEYS: tuple[str, ...] = ("title", "description")


def load_merged_metadata(
    path: Path,
    catalog_root: Path,
) -> dict[str, Any]:
    """Load merged metadata.yaml from hierarchy."""
    return copy.deepcopy(merge_metadata_files(path, catalog_root))


def merge_metadata_files(
    path: Path, catalog_root: Path, read: YamlReader = parse_yaml
) -> dict[str, Any]:
    """Merge the metadata.yaml files from ``catalog_root`` down to ``path``.

    Inherited :data:`NON_INHERITED_METADATA_KEYS` drop out; do not mutate the result.
    """
    count("config.merges")
    merged = _merge_files(find_portolan_files(path, METADATA_FILENAME, catalog_root), read)
    if not merged:
        return merged
    own = own_metadata(path, read)
    for key in NON_INHERITED_METADATA_KEYS:
        if key in merged and key not in own:
            del merged[key]
    return merged


def own_metadata(path: Path, read: YamlReader = parse_yaml) -> dict[str, Any]:
    """Return the metadata.yaml that ``path`` owns, shared with ``read``. Do not mutate it."""
    file_path = path / ".portolan" / METADATA_FILENAME
    if not file_path.is_file():
        return {}
    return _load_validated_mapping(file_path, read) or {}


def load_own_metadata(path: Path) -> dict[str, Any]:
    """Load only the metadata.yaml a directory owns, with no inheritance."""
    return copy.deepcopy(own_metadata(path))


def check_sensitive_settings_in_config(
    catalog_path: Path, *, state: ConfigSource | None = None
) -> list[str]:
    """Check if config.yaml contains sensitive settings that should be in .env."""
    config = load_catalog_config(catalog_path, state=state).data

    # Check top-level sensitive settings
    found = [key for key in SENSITIVE_SETTINGS if key in config]

    # Check collection-level sensitive settings
    collections = config.get("collections", {})
    for collection_name, collection_conf in collections.items():
        if isinstance(collection_conf, dict):
            found.extend(
                f"collections.{collection_name}.{key}"
                for key in SENSITIVE_SETTINGS
                if key in collection_conf
            )

    return found


def coerce_bool(value: Any, *, default: bool = False) -> bool:
    """Coerce a config value to boolean."""
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ("true", "1", "yes", "on")
    return bool(value)


def coerce_int(value: Any, *, default: int) -> int:
    """Coerce a config value to integer with a safe fallback."""
    if value is None:
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def resolve_push_settings(
    destination: str | None,
    profile: str | None,
    catalog_path: Path,
    collection: str | None,
) -> tuple[str | None, str | None, str | None]:
    """Resolve remote/profile/region for push/sync with CLI > env > config precedence.

    Security: these never come from ``config.yaml``, which gets pushed; a stale one raises.
    """
    collection_path = catalog_path / collection if collection else None
    resolved_destination = get_setting(
        "remote",
        cli_value=destination,
        catalog_path=catalog_path,
        collection=collection,
        collection_path=collection_path,
    )
    resolved_profile = get_setting(
        "aws_profile",
        cli_value=profile,
        catalog_path=catalog_path,
        collection=collection,
        collection_path=collection_path,
    )
    resolved_region = get_setting(
        "region",
        cli_value=None,
        catalog_path=catalog_path,
        collection=collection,
        collection_path=collection_path,
    )
    return resolved_destination, resolved_profile, resolved_region
