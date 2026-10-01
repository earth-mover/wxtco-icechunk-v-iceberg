"""DuckDB connection with the Iceberg extension loaded from PyPI wheels."""

from __future__ import annotations

import importlib
from pathlib import Path

import duckdb

# Avro must load first; the iceberg extension depends on it.
_EXTENSION_MODULES = ("duckdb_extension_avro", "duckdb_extension_iceberg")


def _wheel_extension(module_name: str) -> Path | None:
    """Find the .duckdb_extension file shipped in an installed extension wheel."""
    try:
        module = importlib.import_module(module_name)
    except ImportError:
        return None
    if not module.__file__:
        return None
    found = sorted(Path(module.__file__).parent.glob("extensions/*/*.duckdb_extension"))
    # A wheel can ship several builds. Prefer the one for this duckdb version.
    wanted = f"v{duckdb.__version__}"
    return next((p for p in found if p.parent.name == wanted), found[0] if found else None)


def connect_iceberg() -> duckdb.DuckDBPyConnection:
    """Give a DuckDB connection that can read Iceberg. Wheels first, network install as fallback."""
    con = duckdb.connect()
    paths = [_wheel_extension(name) for name in _EXTENSION_MODULES]
    if all(paths):
        try:
            for path in paths:
                con.execute(f"LOAD '{path}'")
            return con
        # A wheel built for another duckdb version fails to load. Fall back to the network.
        except duckdb.Error:
            pass
    con.execute("INSTALL iceberg; LOAD iceberg;")
    return con
