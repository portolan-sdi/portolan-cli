"""Settings of the Iceberg backend, read from the process environment.

The backend writes Iceberg tables through a REST catalog. DuckDB, the
writer, attaches a catalog by its REST endpoint and reads the data files
through an S3 secret, so the backend needs the endpoint, the catalog
credential, and the storage credential. All of them come from environment
variables:

``PORTOLAN_ICEBERG_URI``
    The REST catalog, such as ``https://catalog.example.org`` or the e2e
    stack's ``http://localhost:8181``. Required.
``PORTOLAN_ICEBERG_WAREHOUSE``
    The warehouse the catalog serves, passed to ``GET /v1/config`` and to
    DuckDB's ``ATTACH``. Each catalog service names its warehouses its own
    way; the service's documentation gives the value.
``PORTOLAN_ICEBERG_NAMESPACE``
    The namespace that holds one table per collection. Default ``portolake``.
``PORTOLAN_ICEBERG_AUTH``
    ``oauth2`` (the client credentials grant the Iceberg REST specification
    defines), ``bearer`` (an access token minted by an identity provider), or
    ``none`` (a catalog with no authentication, such as the e2e stack). When
    unset, a client id selects ``oauth2``, a token selects ``bearer``, and
    nothing selects ``none``.
``PORTOLAN_ICEBERG_CLIENT_ID``, ``PORTOLAN_ICEBERG_CLIENT_SECRET``,
``PORTOLAN_ICEBERG_TOKEN``
    The credentials. Secrets stay in the environment and out of ``repr``.
``PORTOLAN_ICEBERG_OAUTH2_SERVER_URI``
    The token endpoint. Default ``<uri>/v1/oauth/tokens``.
``PORTOLAN_ICEBERG_STORAGE``
    ``s3`` (default), ``vended`` (take the storage credential from the
    catalog's ``loadTable`` response, the mechanism the REST specification
    defines for a catalog that fronts the object store), or ``gcs``
    (reserved for a catalog whose data lives on Google Cloud Storage).

The S3 settings come from the conventions ``upload_file`` already follows
(``PORTOLAN_S3_ENDPOINT``, ``PORTOLAN_S3_USE_SSL``, the ``AWS_*`` variables,
and the AWS profile), so one shell setup serves the STAC upload and the
warehouse. The catalog credential and the storage credential stay separate.
A service that issues one key pair for both takes it twice: as the client
pair and as the AWS key pair.

Why the environment and not ``.portolan/config.yaml``: ``init --backend
iceberg`` checks the catalog before the config file exists, and a secret
must never land in a file the catalog publishes. A config key for the
non-secret values is a later addition.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Literal

from portolan_cli.api import resolve_s3_settings

ENV_PREFIX = "PORTOLAN_ICEBERG_"
DEFAULT_NAMESPACE = "portolake"
AUTH_MODES: tuple[str, ...] = ("oauth2", "bearer", "none")
STORAGE_KINDS: tuple[str, ...] = ("s3", "vended", "gcs")

AuthMode = Literal["oauth2", "bearer", "none"]
StorageKind = Literal["s3", "vended", "gcs"]


class IcebergSettingsError(ValueError):
    """A setting is missing or has a value the backend cannot use.

    A ``ValueError`` subclass, because the backend factory passes one through
    unchanged to the command, which reports the class name in its JSON error
    envelope.
    """


@dataclass(frozen=True)
class StorageSettings:
    """The object store that holds the warehouse, as DuckDB's S3 secret needs it.

    ``endpoint`` is a host with an optional port and no scheme. ``use_ssl``
    carries the scheme. ``url_style`` is ``path`` whenever an endpoint is
    set, as ``upload_file`` does for an S3-compatible service.
    """

    kind: StorageKind
    endpoint: str | None
    use_ssl: bool
    region: str | None
    url_style: Literal["path", "vhost"]
    key_id: str | None
    secret: str | None = field(repr=False)
    session_token: str | None = field(repr=False)


@dataclass(frozen=True)
class IcebergSettings:
    """Everything the backend needs to reach one REST catalog."""

    uri: str
    warehouse: str | None
    namespace: str
    auth: AuthMode
    oauth2_server_uri: str | None
    client_id: str | None
    client_secret: str | None = field(repr=False)
    token: str | None = field(repr=False)
    storage: StorageSettings = field(kw_only=True)


def _variable(name: str) -> str:
    return f"{ENV_PREFIX}{name}"


def _read(name: str) -> str | None:
    """Return the variable's value, or None when it is unset.

    A variable that is set to blank is an error. It is almost always a
    typo in a shell export, and the backend must not fall back to a default
    silently.
    """
    variable = _variable(name)
    value = os.environ.get(variable)
    if value is None:
        return None
    if not value.strip():
        raise IcebergSettingsError(f"{variable} is set but empty.")
    return value.strip()


def _choice(name: str, value: str | None, allowed: tuple[str, ...], default: str) -> str:
    if value is None:
        return default
    if value not in allowed:
        raise IcebergSettingsError(
            f"{_variable(name)}={value!r} is not one of {', '.join(allowed)}."
        )
    return value


def _strip_scheme(endpoint: str) -> tuple[str, bool | None]:
    """Return the endpoint host and the TLS setting its scheme implies, if any."""
    if endpoint.startswith("https://"):
        return endpoint[len("https://") :], True
    if endpoint.startswith("http://"):
        return endpoint[len("http://") :], False
    return endpoint, None


def _resolve_auth(uri: str) -> tuple[AuthMode, str | None, str | None, str | None, str | None]:
    client_id = _read("CLIENT_ID")
    client_secret = _read("CLIENT_SECRET")
    token = _read("TOKEN")
    explicit = _read("AUTH")
    if explicit is None:
        inferred = "oauth2" if client_id else "bearer" if token else "none"
    else:
        inferred = _choice("AUTH", explicit, AUTH_MODES, "none")
    auth: AuthMode = inferred  # type: ignore[assignment]
    oauth2_server_uri = _read("OAUTH2_SERVER_URI")
    if auth == "oauth2":
        missing = [
            _variable(name)
            for name, value in (("CLIENT_ID", client_id), ("CLIENT_SECRET", client_secret))
            if value is None
        ]
        if missing:
            raise IcebergSettingsError(f"{_variable('AUTH')}=oauth2 needs {' and '.join(missing)}.")
        if oauth2_server_uri is None:
            oauth2_server_uri = f"{uri}/v1/oauth/tokens"
    elif auth == "bearer" and token is None:
        raise IcebergSettingsError(f"{_variable('AUTH')}=bearer needs {_variable('TOKEN')}.")
    return auth, oauth2_server_uri, client_id, client_secret, token


def _resolve_storage(profile: str | None) -> StorageSettings:
    kind_value = _choice("STORAGE", _read("STORAGE"), STORAGE_KINDS, "s3")
    kind: StorageKind = kind_value  # type: ignore[assignment]
    s3 = resolve_s3_settings(profile=profile)
    endpoint: str | None = None
    use_ssl = s3.use_ssl
    if s3.endpoint is not None:
        endpoint, implied = _strip_scheme(s3.endpoint)
        if implied is not None and os.environ.get("PORTOLAN_S3_USE_SSL") is None:
            use_ssl = implied
    key_id, secret, session_token = s3.access_key, s3.secret_key, s3.session_token
    if kind == "vended":
        key_id = secret = session_token = None
    return StorageSettings(
        kind=kind,
        endpoint=endpoint,
        use_ssl=use_ssl,
        region=s3.region,
        url_style="path" if endpoint else "vhost",
        key_id=key_id,
        secret=secret,
        session_token=session_token,
    )


def load_settings(*, profile: str | None = None) -> IcebergSettings:
    """Read the backend settings from the environment.

    Args:
        profile: An AWS profile for the S3 settings, or None.

    Returns:
        The settings.

    Raises:
        IcebergSettingsError: A required variable is missing, or a value is
            outside its allowed set. The message names the variable.
    """
    uri = _read("URI")
    if uri is None:
        raise IcebergSettingsError(
            f"The iceberg backend needs {_variable('URI')}, the Iceberg REST catalog "
            "to write to, for example https://catalog.example.org."
        )
    uri = uri.rstrip("/")
    namespace = _read("NAMESPACE") or DEFAULT_NAMESPACE
    auth, oauth2_server_uri, client_id, client_secret, token = _resolve_auth(uri)
    return IcebergSettings(
        uri=uri,
        warehouse=_read("WAREHOUSE"),
        namespace=namespace,
        auth=auth,
        oauth2_server_uri=oauth2_server_uri,
        client_id=client_id,
        client_secret=client_secret,
        token=token,
        storage=_resolve_storage(profile),
    )
