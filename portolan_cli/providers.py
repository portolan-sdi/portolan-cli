"""The provider model behind a collection's STAC ``providers`` array."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any, Literal

from portolan_core.errors import InvalidProvidersError

PRODUCER_ROLE = "producer"
HOST_ROLE = "host"

#: The four roles STAC defines for a provider.
PROVIDER_ROLES: tuple[str, ...] = ("producer", "processor", "licensor", "host")

#: Optional provider fields carried through from metadata.yaml. ``email`` is not
#: a core STAC field but is a valid extension of the provider object, and the
#: Portolan schema accepts it in place of ``url`` on the host (PTL-PRV-003).
_OPTIONAL_FIELDS = ("description", "url", "email")

Provenance = Literal["official", "mirror"]


def _text(value: object) -> str | None:
    """Return a stripped non-empty string, or None for anything else."""
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _contact_text(value: object) -> str | None:
    """Return a contact value, or None for a seeded ``TODO`` placeholder."""
    text = _text(value)
    # The word TODO, not a name such as "Todos Santos" that starts with it.
    if text is None or re.match(r"todo\b", text, re.IGNORECASE):
        return None
    return text


def _roles_of(provider: Mapping[str, Any]) -> list[str]:
    raw = provider.get("roles")
    if not isinstance(raw, list):
        return []
    return [role for role in raw if isinstance(role, str)]


def _with_role(providers: list[dict[str, Any]], role: str) -> list[dict[str, Any]]:
    return [provider for provider in providers if role in _roles_of(provider)]


def _normalized_name(provider: Mapping[str, Any]) -> str | None:
    name = _text(provider.get("name"))
    return name.casefold() if name is not None else None


def _validated_roles(entry: Mapping[str, Any], index: int) -> list[str]:
    """Validate one entry's roles against the four STAC defines."""
    raw = entry.get("roles")
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise InvalidProvidersError(f"providers[{index}].roles must be a list")

    for role in raw:
        if not isinstance(role, str) or role not in PROVIDER_ROLES:
            raise InvalidProvidersError(
                f"providers[{index}].roles has unknown role {role!r}; "
                f"use one of {', '.join(PROVIDER_ROLES)}"
            )
    return list(raw)


def _validated_entry(entry: object, index: int) -> dict[str, Any]:
    """Turn one metadata.yaml providers entry into a STAC provider object."""
    if not isinstance(entry, Mapping):
        raise InvalidProvidersError(f"providers[{index}] must be a mapping with a 'name'")

    name = _text(entry.get("name"))
    if name is None:
        raise InvalidProvidersError(f"providers[{index}].name cannot be empty")

    provider: dict[str, Any] = {"name": name}
    roles = _validated_roles(entry, index)
    if roles:
        provider["roles"] = roles
    for field in _OPTIONAL_FIELDS:
        value = _text(entry.get(field))
        if value is not None:
            provider[field] = value
    return provider


def _host_from_contact_text(text: str) -> dict[str, Any]:
    """Treat a bare ``contact`` string as the maintainer, and as an address if it is one."""
    host: dict[str, Any] = {"name": text, "roles": [HOST_ROLE]}
    if "@" in text:
        host["email"] = text
    return host


def _host_from_contact_mapping(contact: Mapping[str, Any]) -> dict[str, Any] | None:
    email = _contact_text(contact.get("email"))
    name = _contact_text(contact.get("name"))
    if name is None and email is None:
        return None

    host: dict[str, Any] = {"name": name or email, "roles": [HOST_ROLE]}
    if email is not None:
        host["email"] = email
    url = _contact_text(contact.get("url"))
    if url is not None:
        host["url"] = url
    return host


def _host_from_contact(contact: object) -> dict[str, Any] | None:
    """Build a host provider from metadata.yaml's ``contact``."""
    if isinstance(contact, str):
        text = _contact_text(contact)
        return _host_from_contact_text(text) if text is not None else None
    if isinstance(contact, Mapping):
        return _host_from_contact_mapping(contact)
    return None


def _declared_host(providers: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Return the one entry claiming the host role, refusing to pick between two."""
    hosts = _with_role(providers, HOST_ROLE)
    if len(hosts) > 1:
        names = ", ".join(repr(host["name"]) for host in hosts)
        raise InvalidProvidersError(
            f"exactly one provider may carry the 'host' role, but {len(hosts)} do: {names}"
        )
    return hosts[0] if hosts else None


def resolve_providers(metadata: object) -> list[dict[str, Any]] | None:
    """Build a collection's STAC providers array from merged metadata.yaml."""
    if not isinstance(metadata, Mapping):
        return None

    raw = metadata.get("providers")
    providers: list[dict[str, Any]] = []
    if isinstance(raw, list):
        providers = [_validated_entry(entry, index) for index, entry in enumerate(raw)]

    host = _declared_host(providers)
    if host is None:
        host = _host_from_contact(metadata.get("contact"))
        if host is not None:
            providers.append(host)

    if not providers:
        return None
    if host is None:
        return providers
    return [provider for provider in providers if provider is not host] + [host]


def derive_provenance(providers: list[dict[str, Any]] | None) -> Provenance | None:
    """Derive whether a collection is official or a mirror of someone else's data."""
    if not providers:
        return None

    producers = _with_role(providers, PRODUCER_ROLE)
    hosts = _with_role(providers, HOST_ROLE)
    if not producers or len(hosts) != 1:
        return None

    host_name = _normalized_name(hosts[0])
    if host_name is None:
        return None
    if any(_normalized_name(producer) == host_name for producer in producers):
        return "official"
    return "mirror"
