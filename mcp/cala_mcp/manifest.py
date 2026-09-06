"""Reader for dbt's manifest.json.

The manifest is the single source of truth for what the warehouse contains:
which models exist, where they live (relation_name), which columns they
declare and what they mean, their grain (config.meta.grain) and lineage. The
tools ask the manifest for every identifier they put into SQL, and
`Relation.require()` refuses columns the manifest does not declare, so a
query can never reference something undocumented.

If the manifest is missing the loader says so and stops; it never guesses.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any

from cala_mcp.paths import DEFAULT_MANIFEST


class ManifestError(Exception):
    """The manifest is missing, unreadable, or does not know a name we need."""


class ManifestMissing(ManifestError):
    pass


class UnknownRelation(ManifestError):
    pass


class UnknownColumn(ManifestError):
    pass


@dataclass(frozen=True)
class Column:
    name: str
    description: str
    tests: tuple[str, ...] = ()
    accepted_values: tuple[str, ...] | None = None


@dataclass(frozen=True)
class Relation:
    """A model, seed or source table as the manifest describes it."""

    unique_id: str
    name: str
    resource_type: str
    database: str
    schema: str
    identifier: str
    relation_name: str
    description: str
    grain: tuple[str, ...]
    columns: dict[str, Column]
    materialized: str | None
    original_file_path: str
    raw_code: str | None = None
    upstream: tuple[str, ...] = ()
    downstream: tuple[str, ...] = ()
    tests: tuple[str, ...] = field(default_factory=tuple)

    def require(self, *names: str) -> Relation:
        """Assert every name is a declared column; return self for chaining."""
        missing = [n for n in names if n not in self.columns]
        if missing:
            raise UnknownColumn(
                f"{self.name} does not declare column(s) {missing} in the manifest; "
                f"declared: {sorted(self.columns)}. Add them to the model's yml."
            )
        return self

    def col(self, name: str) -> str:
        """The column name, validated against the manifest, ready for SQL."""
        self.require(name)
        return name

    def cols(self, *names: str) -> list[str]:
        return [self.col(n) for n in names]

    def accepted_values(self, column: str) -> tuple[str, ...] | None:
        """Values an accepted_values test allows on the column, or None if untested."""
        return self.columns[self.col(column)].accepted_values

    def check_value(self, column: str, value: str) -> str:
        allowed = self.accepted_values(column)
        if allowed is not None and value not in allowed:
            raise UnknownColumn(
                f"{self.name}.{column} accepts {list(allowed)} (per its accepted_values test), got {value!r}"
            )
        return value


def _column_tests(
    node_id: str, manifest: dict[str, Any]
) -> tuple[dict[str, list[str]], dict[str, tuple[str, ...]]]:
    """Generic tests attached to a node, keyed by column name, plus any
    accepted_values lists so tools can validate inputs without hardcoding."""
    tests: dict[str, list[str]] = {}
    accepted: dict[str, tuple[str, ...]] = {}
    for child_id in manifest.get("child_map", {}).get(node_id, []):
        test = manifest["nodes"].get(child_id)
        if not test or test.get("resource_type") != "test":
            continue
        column = test.get("column_name")
        if not column:
            continue
        meta = test.get("test_metadata") or {}
        kind = meta.get("name") or test["name"]
        tests.setdefault(column, []).append(kind)
        if kind == "accepted_values":
            values = (meta.get("kwargs") or {}).get("values") or []
            accepted[column] = tuple(str(v) for v in values)
    return tests, accepted


def _grain(node: dict[str, Any]) -> tuple[str, ...]:
    meta = (node.get("config") or {}).get("meta") or node.get("meta") or {}
    grain = meta.get("grain") or []
    if isinstance(grain, str):
        grain = [grain]
    return tuple(grain)


class Manifest:
    def __init__(self, raw: dict[str, Any], path: Path):
        self.path = path
        self.raw = raw
        self.metadata = raw.get("metadata", {})
        self._by_name: dict[str, Relation] = {}
        self._by_id: dict[str, Relation] = {}
        self._index()

    # ----------------------------------------------------------------- loading
    @classmethod
    def load(cls, path: Path | str = DEFAULT_MANIFEST) -> Manifest:
        path = Path(path)
        if not path.exists():
            raise ManifestMissing(
                f"dbt manifest not found at {path}. Run `make build` first; the tools "
                "read model names, columns and lineage from it and will not guess."
            )
        return _load_cached(str(path), path.stat().st_mtime_ns)

    # ---------------------------------------------------------------- indexing
    def _index(self) -> None:
        nodes = self.raw.get("nodes", {})
        sources = self.raw.get("sources", {})
        parent_map = self.raw.get("parent_map", {})
        child_map = self.raw.get("child_map", {})

        def name_of(uid: str) -> str | None:
            node = nodes.get(uid) or sources.get(uid)
            return node["name"] if node else None

        def rel(uid: str, node: dict[str, Any]) -> Relation:
            tests_by_col, accepted_by_col = _column_tests(uid, self.raw)
            columns = {
                c["name"]: Column(
                    name=c["name"],
                    description=(c.get("description") or "").strip(),
                    tests=tuple(tests_by_col.get(c["name"], [])),
                    accepted_values=accepted_by_col.get(c["name"]),
                )
                for c in (node.get("columns") or {}).values()
            }
            upstream = tuple(n for n in (name_of(p) for p in parent_map.get(uid, [])) if n)
            downstream = []
            tests = []
            for cid in child_map.get(uid, []):
                child = nodes.get(cid)
                if not child:
                    continue
                if child["resource_type"] == "test":
                    tests.append(child["name"])
                else:
                    downstream.append(child["name"])
            config = node.get("config") or {}
            return Relation(
                unique_id=uid,
                name=node["name"],
                resource_type=node["resource_type"],
                database=node.get("database") or "",
                schema=node.get("schema") or "",
                identifier=node.get("alias") or node.get("identifier") or node["name"],
                relation_name=node.get("relation_name") or "",
                description=(node.get("description") or "").strip(),
                grain=_grain(node),
                columns=columns,
                materialized=config.get("materialized"),
                original_file_path=node.get("original_file_path") or "",
                raw_code=node.get("raw_code"),
                upstream=upstream,
                downstream=tuple(downstream),
                tests=tuple(tests),
            )

        for uid, node in nodes.items():
            if node.get("resource_type") in ("model", "seed", "snapshot"):
                r = rel(uid, node)
                self._by_id[uid] = r
                self._by_name[r.name] = r
        for uid, node in sources.items():
            r = rel(uid, node)
            self._by_id[uid] = r
            # models win on a name clash; sources are reachable via source()
            self._by_name.setdefault(r.name, r)

    # ----------------------------------------------------------------- queries
    def names(self, resource_type: str | None = None) -> list[str]:
        return sorted(
            r.name for r in self._by_name.values() if resource_type is None or r.resource_type == resource_type
        )

    def model(self, name: str) -> Relation:
        """A model (or seed / source, by name) the manifest knows about."""
        r = self._by_name.get(name)
        if r is None:
            raise UnknownRelation(
                f"{name!r} is not a model in the manifest. Models: {self.names('model')}"
            )
        return r

    def source(self, table: str) -> Relation:
        for r in self._by_id.values():
            if r.resource_type == "source" and r.name == table:
                return r
        raise UnknownRelation(
            f"{table!r} is not a source table in the manifest. Sources: {self.names('source')}"
        )

    def test(self, name: str) -> dict[str, Any]:
        for uid, node in self.raw.get("nodes", {}).items():
            if node.get("resource_type") == "test" and node["name"] == name:
                return node
        raise UnknownRelation(f"{name!r} is not a test in the manifest.")


@lru_cache(maxsize=4)
def _load_cached(path: str, mtime_ns: int) -> Manifest:
    with open(path, encoding="utf-8") as fh:
        raw = json.load(fh)
    if "nodes" not in raw or "metadata" not in raw:
        raise ManifestError(f"{path} does not look like a dbt manifest (no nodes/metadata).")
    return Manifest(raw, Path(path))
