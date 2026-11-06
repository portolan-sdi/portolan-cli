"""An in-process Iceberg REST catalog and a DuckDB stand-in, for the backend's unit tests.

``FakeCatalog`` answers the REST calls the backend makes and applies the
``commitTable`` updates it sends: properties, refs, snapshot removal,
partition specs. ``FakeConnection`` answers the SQL the backend sends to
DuckDB and turns each DDL or DML statement into a change of the same
catalog: ``CREATE TABLE`` makes a table, ``TRUNCATE`` and ``INSERT`` make
snapshots, ``DESCRIBE`` answers from the table schema or from a map of
source files. Together they let a test run ``publish``, ``rollback``, and
``prune`` end to end with no network and no DuckDB, and assert the exact
requests the backend made.

The double mirrors what the probe of 2026-11-04 saw on DuckDB 1.5.6: a
truncate-plus-insert transaction commits two snapshots, a delete and an
append, and ``main`` ends on the append.
"""

from __future__ import annotations

import json
import re
from typing import TYPE_CHECKING, Any

import httpx

from portolan_cli.backends.iceberg.backend import IcebergBackend
from portolan_cli.backends.iceberg.settings import IcebergSettings, StorageSettings

if TYPE_CHECKING:
    from pathlib import Path

    from portolan_cli.backends.protocol import SchemaFingerprint

# DuckDB type strings for the Iceberg types the fake keeps, both ways.
_ICEBERG_TO_DUCKDB = {
    "long": "BIGINT",
    "int": "INTEGER",
    "double": "DOUBLE",
    "float": "FLOAT",
    "string": "VARCHAR",
    "boolean": "BOOLEAN",
    "binary": "BLOB",
    "date": "DATE",
    "timestamp": "TIMESTAMP",
}
_DUCKDB_TO_ICEBERG = {value: key for key, value in _ICEBERG_TO_DUCKDB.items()}


def duckdb_type(iceberg_type: str) -> str:
    """Return the DuckDB type of an Iceberg type string, as DESCRIBE of the table reports it.

    DuckDB reads an Iceberg geometry column back as ``GEOMETRY`` with no CRS
    (seen on 1.5.6 against the Apache REST fixture).
    """
    if iceberg_type.startswith("geometry"):
        return "GEOMETRY"
    return _ICEBERG_TO_DUCKDB.get(iceberg_type, iceberg_type.upper())


def iceberg_type(duckdb_type_name: str) -> str:
    """Return the Iceberg type string of a DuckDB type."""
    match = re.fullmatch(r"GEOMETRY\('([^']+)'\)", duckdb_type_name)
    if match:
        return f"geometry({match.group(1)})"
    return _DUCKDB_TO_ICEBERG.get(duckdb_type_name, duckdb_type_name.lower())


class FakeCatalog:
    """The subset of the Iceberg REST catalog the backend uses, with state."""

    def __init__(self, *, prefix: str = "", prefix_in: str = "overrides") -> None:
        self.prefix = prefix
        self.prefix_in = prefix_in
        self.namespaces: set[str] = set()
        self.tables: dict[tuple[str, str], dict[str, Any]] = {}
        self.requests: list[httpx.Request] = []
        self.commits: list[dict[str, Any]] = []
        #: Statuses to answer with before the real answer, for retry tests.
        self.failures: list[int] = []
        #: A status every commit answers with, for conflict tests.
        self.commit_status: int | None = None
        #: The ``config`` of every loadTable answer.
        self.table_config: dict[str, str] = {}
        self.clock_ms = 1_700_000_000_000
        self._next_snapshot = 1000
        self._next_field = 1

    # ---------------------------------------------------------------- state

    def client(self) -> httpx.Client:
        """An HTTP client whose transport is this catalog."""
        return httpx.Client(transport=httpx.MockTransport(self))

    def add_table(self, namespace: str, name: str, metadata: dict[str, Any]) -> None:
        """Add a table with a metadata document as given, for the REST client tests."""
        self.namespaces.add(namespace)
        self.tables[(namespace, name)] = {"location": 1, "metadata": metadata}

    def create_table(
        self, namespace: str, name: str, columns: list[tuple[str, str]], *, format_version: int = 3
    ) -> None:
        """Create an empty table. ``columns`` holds ``(name, iceberg type)`` pairs."""
        self.namespaces.add(namespace)
        fields = [
            {"id": self._field_id(), "name": column, "required": False, "type": kind}
            for column, kind in columns
        ]
        self.tables[(namespace, name)] = {
            "location": 1,
            "metadata": {
                "format-version": format_version,
                "table-uuid": f"uuid-{namespace}-{name}",
                "location": f"s3://warehouse/{namespace}/{name}",
                "current-schema-id": 0,
                "schemas": [{"schema-id": 0, "type": "struct", "fields": fields}],
                "default-spec-id": 0,
                "partition-specs": [{"spec-id": 0, "fields": []}],
                "current-snapshot-id": -1,
                "refs": {},
                "snapshots": [],
                "properties": {},
            },
        }

    def metadata(self, namespace: str, name: str) -> dict[str, Any]:
        """The metadata document of a table."""
        document: dict[str, Any] = self.tables[(namespace, name)]["metadata"]
        return document

    def metadata_location(self, namespace: str, name: str) -> str:
        table = self.tables[(namespace, name)]
        return f"s3://warehouse/{namespace}/{name}/metadata/v{table['location']}.metadata.json"

    def add_snapshot(self, namespace: str, name: str, operation: str) -> int:
        """Append a snapshot and move ``main`` to it, as a DuckDB commit does."""
        document = self.metadata(namespace, name)
        snapshot_id = self._next_snapshot
        self._next_snapshot += 1
        self.clock_ms += 1000
        parent = document.get("current-snapshot-id")
        document["snapshots"].append(
            {
                "snapshot-id": snapshot_id,
                "parent-snapshot-id": parent if parent != -1 else None,
                "timestamp-ms": self.clock_ms,
                "summary": {"operation": operation},
            }
        )
        document["current-snapshot-id"] = snapshot_id
        document["refs"]["main"] = {"snapshot-id": snapshot_id, "type": "branch"}
        self.tables[(namespace, name)]["location"] += 1
        return snapshot_id

    def add_column(self, namespace: str, name: str, column: str, kind: str) -> None:
        document = self.metadata(namespace, name)
        schema = dict(document["schemas"][-1])
        schema["schema-id"] = schema["schema-id"] + 1
        schema["fields"] = [
            *schema["fields"],
            {"id": self._field_id(), "name": column, "required": False, "type": kind},
        ]
        document["schemas"].append(schema)
        document["current-schema-id"] = schema["schema-id"]

    def columns(self, namespace: str, name: str) -> list[tuple[str, str]]:
        """The ``(name, iceberg type)`` pairs of the current schema."""
        document = self.metadata(namespace, name)
        schema = next(
            s for s in document["schemas"] if s["schema-id"] == document["current-schema-id"]
        )
        return [(f["name"], f["type"]) for f in schema["fields"]]

    def _field_id(self) -> int:
        self._next_field += 1
        return self._next_field

    # ---------------------------------------------------------------- transport

    def __call__(self, request: httpx.Request) -> httpx.Response:  # noqa: C901, PLR0911, PLR0912
        self.requests.append(request)
        if self.failures:
            return httpx.Response(self.failures.pop(0), json={"error": {"message": "later"}})
        path = request.url.path
        if path == "/v1/config":
            body: dict[str, Any] = {"defaults": {}, "overrides": {}}
            if self.prefix:
                body[self.prefix_in] = {"prefix": self.prefix}
            return httpx.Response(200, json=body)
        base = f"/v1/{self.prefix}" if self.prefix else "/v1"
        if not path.startswith(f"{base}/namespaces"):
            return httpx.Response(404, json={"error": {"message": f"no route {path}"}})
        parts = path[len(f"{base}/namespaces") :].strip("/").split("/")
        if parts == [""]:
            if request.method == "POST":
                namespace = json.loads(request.content)["namespace"][0]
                if namespace in self.namespaces:
                    return httpx.Response(409, json={"error": {"message": "exists"}})
                self.namespaces.add(namespace)
                return httpx.Response(200, json={"namespace": [namespace]})
            return httpx.Response(405)
        namespace = parts[0]
        if len(parts) == 1:
            if request.method == "HEAD":
                return httpx.Response(204 if namespace in self.namespaces else 404)
            if request.method == "DELETE":
                self.namespaces.discard(namespace)
                return httpx.Response(204)
            return httpx.Response(405)
        if len(parts) == 2:
            names = [n for (ns, n) in self.tables if ns == namespace]
            return httpx.Response(
                200, json={"identifiers": [{"namespace": [namespace], "name": n} for n in names]}
            )
        key = (namespace, parts[2])
        if request.method == "HEAD":
            return httpx.Response(204 if key in self.tables else 404)
        if key not in self.tables:
            return httpx.Response(404, json={"error": {"message": "NoSuchTableException"}})
        if request.method == "GET":
            return httpx.Response(200, json=self._load(key))
        if request.method == "DELETE":
            del self.tables[key]
            return httpx.Response(204)
        if request.method == "POST":
            return self._commit(key, json.loads(request.content))
        return httpx.Response(405)

    def _load(self, key: tuple[str, str]) -> dict[str, Any]:
        return {
            "metadata-location": self.metadata_location(*key),
            "metadata": self.tables[key]["metadata"],
            "config": dict(self.table_config),
        }

    def _commit(self, key: tuple[str, str], body: dict[str, Any]) -> httpx.Response:
        self.commits.append(body)
        if self.commit_status is not None:
            return httpx.Response(self.commit_status, json={"error": {"message": "conflict"}})
        document = self.tables[key]["metadata"]
        for requirement in body.get("requirements", []):
            failure = self._check(document, requirement)
            if failure is not None:
                return httpx.Response(409, json={"error": {"message": failure}})
        for update in body.get("updates", []):
            self._apply(document, update)
        self.tables[key]["location"] += 1
        return httpx.Response(200, json=self._load(key))

    @staticmethod
    def _check(document: dict[str, Any], requirement: dict[str, Any]) -> str | None:
        kind = requirement["type"]
        if kind == "assert-table-uuid":
            return None if requirement["uuid"] == document["table-uuid"] else "uuid mismatch"
        if kind == "assert-ref-snapshot-id":
            ref = document.get("refs", {}).get(requirement["ref"])
            found = ref["snapshot-id"] if ref else None
            if found is None and requirement["ref"] == "main":
                # A document with no refs section still names its current snapshot.
                current = document.get("current-snapshot-id")
                found = None if current in (None, -1) else current
            expected = requirement.get("snapshot-id")
            if found != expected:
                return f"ref {requirement['ref']} is at {found}, not {expected}"
            return None
        return f"unsupported requirement {kind}"

    @staticmethod
    def _apply(document: dict[str, Any], update: dict[str, Any]) -> None:  # noqa: C901
        action = update["action"]
        if action == "set-properties":
            document.setdefault("properties", {}).update(update["updates"])
        elif action == "remove-properties":
            for name in update["removals"]:
                document.setdefault("properties", {}).pop(name, None)
        elif action == "set-snapshot-ref":
            document.setdefault("refs", {})[update["ref-name"]] = {
                "snapshot-id": update["snapshot-id"],
                "type": update["type"],
            }
            if update["ref-name"] == "main":
                document["current-snapshot-id"] = update["snapshot-id"]
        elif action == "remove-snapshot-ref":
            document.setdefault("refs", {}).pop(update["ref-name"], None)
        elif action == "remove-snapshots":
            gone = set(update["snapshot-ids"])
            document["snapshots"] = [
                s for s in document.get("snapshots", []) if s["snapshot-id"] not in gone
            ]
        elif action == "add-spec":
            document.setdefault("partition-specs", []).append(update["spec"])
        elif action == "set-default-spec":
            spec_id = update["spec-id"]
            if spec_id == -1:
                spec_id = document["partition-specs"][-1]["spec-id"]
            document["default-spec-id"] = spec_id
        else:
            raise AssertionError(f"the fake catalog does not apply {action}")


_TABLE = r'lake\."(?P<ns>[^"]+)"\."(?P<name>[^"]+)"'
_CREATE = re.compile(
    rf"CREATE TABLE {_TABLE} \((?P<cols>.*)\) WITH \('format-version' = (?P<fv>\d)\)"
)
_ADD_COLUMN = re.compile(rf'ALTER TABLE {_TABLE} ADD COLUMN "(?P<col>[^"]+)" (?P<kind>.+)')
_TRUNCATE = re.compile(rf"TRUNCATE {_TABLE}")
_INSERT = re.compile(
    rf"INSERT INTO {_TABLE} BY NAME SELECT \* FROM read_parquet\(\[(?P<files>.*)\], "
)
_DESCRIBE_TABLE = re.compile(rf"DESCRIBE SELECT \* FROM {_TABLE}")
_DESCRIBE_FILES = re.compile(r"DESCRIBE SELECT \* FROM read_parquet\(\[(?P<files>.*)\], ")
_COUNT = re.compile(rf"SELECT count\(\*\) FROM {_TABLE}")
_FILES = re.compile(r"FROM iceberg_metadata\('(?P<loc>[^']+)', snapshot_from_id => (?P<sid>\d+)\)")
_COLUMN = re.compile(r'"((?:[^"]|"")+)" (.+)')


def _quoted_list(text: str) -> list[str]:
    return [m.group(1).replace("''", "'") for m in re.finditer(r"'((?:[^']|'')*)'", text)]


class FakeConnection:
    """Answers the backend's SQL and turns its writes into catalog changes.

    Attributes:
        statements: Every statement, in order.
        sources: Source file path -> ``(columns, rows)``, where columns are
            ``(name, duckdb type)`` pairs. ``DESCRIBE`` of a file list unions
            the columns by name, as ``union_by_name`` does.
        files: ``(namespace, name, snapshot id)`` -> the ``iceberg_metadata``
            rows of that snapshot, as ``(manifest_content, status, content, file_path)``.
    """

    def __init__(self, catalog: FakeCatalog) -> None:
        self.catalog = catalog
        self.statements: list[str] = []
        self.sources: dict[str, tuple[list[tuple[str, str]], int]] = {}
        self.files: dict[tuple[str, str, int], list[tuple[str, str, str, str]]] = {}
        self.row_counts: dict[tuple[str, str], int] = {}
        self._rows: list[tuple[Any, ...]] = []
        self._pending: list[tuple[str, str, str, int]] | None = None
        self.closed = False

    def execute(self, sql: str, params: Any = None) -> FakeConnection:  # noqa: C901, PLR0912
        self.statements.append(sql)
        self._rows = []
        if sql == "BEGIN":
            self._pending = []
        elif sql == "COMMIT":
            for ns, name, operation, rows in self._pending or []:
                self._write(ns, name, operation, rows)
            self._pending = None
        elif sql == "ROLLBACK":
            self._pending = None
        elif match := _CREATE.fullmatch(sql):
            columns = [
                (m.group(1).replace('""', '"'), iceberg_type(m.group(2)))
                for m in (_COLUMN.fullmatch(c.strip()) for c in match.group("cols").split(", "))
                if m
            ]
            self.catalog.create_table(
                match.group("ns"),
                match.group("name"),
                columns,
                format_version=int(match.group("fv")),
            )
            self.row_counts[(match.group("ns"), match.group("name"))] = 0
        elif match := _ADD_COLUMN.fullmatch(sql):
            self.catalog.add_column(
                match.group("ns"),
                match.group("name"),
                match.group("col"),
                iceberg_type(match.group("kind")),
            )
        elif match := _TRUNCATE.fullmatch(sql):
            self._stage(match.group("ns"), match.group("name"), "delete", 0)
        elif match := _INSERT.match(sql):
            rows = sum(self.sources[path][1] for path in _quoted_list(match.group("files")))
            self._stage(match.group("ns"), match.group("name"), "append", rows)
        elif match := _DESCRIBE_TABLE.fullmatch(sql):
            self._rows = [
                (column, duckdb_type(kind))
                for column, kind in self.catalog.columns(match.group("ns"), match.group("name"))
            ]
        elif match := _DESCRIBE_FILES.match(sql):
            merged: dict[str, str] = {}
            for path in _quoted_list(match.group("files")):
                for column, kind in self.sources[path][0]:
                    merged.setdefault(column, kind)
            self._rows = list(merged.items())
        elif match := _COUNT.fullmatch(sql):
            self._rows = [(self.row_counts.get((match.group("ns"), match.group("name")), 0),)]
        elif match := _FILES.search(sql):
            parts = match.group("loc").split("/")
            key = (parts[3], parts[4], int(match.group("sid")))
            self._rows = list(self.files.get(key, []))
        elif sql.startswith(("LOAD ", "INSTALL ", "CREATE SECRET ", "ATTACH ")):
            pass
        else:
            raise AssertionError(f"the fake connection does not answer: {sql}")
        return self

    def _stage(self, ns: str, name: str, operation: str, rows: int) -> None:
        if self._pending is not None:
            self._pending.append((ns, name, operation, rows))
        else:
            self._write(ns, name, operation, rows)

    def _write(self, ns: str, name: str, operation: str, rows: int) -> None:
        self.catalog.add_snapshot(ns, name, operation)
        if operation == "delete":
            self.row_counts[(ns, name)] = 0
        else:
            self.row_counts[(ns, name)] = self.row_counts.get((ns, name), 0) + rows

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self._rows

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._rows[0] if self._rows else None

    def close(self) -> None:
        self.closed = True


#: A schema fingerprint for a publish.
SCHEMA: SchemaFingerprint = {"columns": ["id", "geometry"], "types": {"id": "int64"}, "hash": "h1"}
#: The DuckDB columns of a point GeoParquet file in CRS84.
POINTS = [("id", "BIGINT"), ("name", "VARCHAR"), ("geometry", "GEOMETRY('OGC:CRS84')")]


def _settings(*, use_ssl: bool = True) -> IcebergSettings:
    return IcebergSettings(
        uri="https://catalog.example.org",
        warehouse="wh",
        namespace="portolake",
        auth="none",
        oauth2_server_uri=None,
        client_id=None,
        client_secret=None,
        token=None,
        storage=StorageSettings(
            kind="s3",
            endpoint="s3.example.org",
            use_ssl=use_ssl,
            region="us-east-1",
            url_style="path",
            key_id="k",
            secret="s",  # noqa: S106  # a test value
            session_token=None,
        ),
    )


class Lake:
    """The fake catalog, the fake connection, and a backend on both."""

    def __init__(self, tmp_path: Path, *, use_ssl: bool = True) -> None:
        self.root = tmp_path / "catalog"
        self.root.mkdir()
        self.catalog = FakeCatalog()
        self.connection = FakeConnection(self.catalog)
        self.backend = IcebergBackend(
            catalog_root=self.root,
            settings=_settings(use_ssl=use_ssl),
            http=self.catalog.client(),
            connect=lambda: self.connection,
        )

    def source(
        self, collection: str, name: str, columns: list[tuple[str, str]] = POINTS, rows: int = 3
    ) -> str:
        """Create a (fake) Parquet file under the collection and register its schema."""
        path = self.root / collection / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"parquet " + name.encode())
        self.connection.sources[str(path)] = (columns, rows)
        return str(path)

    def publish(self, collection: str = "roads", **kwargs: Any) -> Any:
        options: dict[str, Any] = {"schema": SCHEMA, "breaking": False, "message": "first"}
        options.update(kwargs)
        if "assets" not in options:
            options["assets"] = {"roads.parquet": self.source(collection, "roads.parquet")}
        return self.backend.publish(collection, **options)

    @property
    def sql(self) -> list[str]:
        return [
            s
            for s in self.connection.statements
            if not s.startswith(("LOAD", "ATTACH", "CREATE SECRET"))
        ]

    @property
    def commits(self) -> list[dict[str, Any]]:
        return self.catalog.commits

    def metadata(self, collection: str = "roads") -> dict[str, Any]:
        return self.catalog.metadata("portolake", collection)
