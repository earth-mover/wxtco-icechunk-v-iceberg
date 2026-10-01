"""Iceberg catalog factories: local sqlite for tests, Arraylake REST for production."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from pyiceberg.catalog import Catalog
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    NoSuchTableError,
    TableAlreadyExistsError,
)
from pyiceberg.table import Table
from pyiceberg.table.sorting import SortField, SortOrder
from pyiceberg.transforms import IdentityTransform

from wxtco.config import Settings
from wxtco.ingest.table_schema import (
    NAMESPACE,
    SORT,
    TABLE_ID,
    TABLE_PROPERTIES,
    iceberg_schema,
    partition_spec,
)

# Source ids of member, lat, lon in the Iceberg schema. Rows arrive already in this order.
_SORT_SOURCE_IDS = (4, 5, 6)


def local_catalog(path: Path) -> Catalog:
    """Give a SqlCatalog on sqlite with a local warehouse below `path`."""
    path.mkdir(parents=True, exist_ok=True)
    return SqlCatalog(
        "local",
        uri=f"sqlite:///{path / 'catalog.db'}",
        warehouse=f"file://{path / 'warehouse'}",
    )


def arraylake_catalog(settings: Settings) -> Catalog:
    """Give the Arraylake REST catalog with vended credentials. Raise if org or token is absent."""
    if not settings.arraylake_token or not settings.arraylake_org:
        raise ValueError("ARRAYLAKE_TOKEN and ARRAYLAKE_ORG are required for the Arraylake catalog")
    return RestCatalog(
        name=settings.arraylake_org,
        uri=settings.iceberg_uri,
        warehouse=settings.arraylake_org,
        token=settings.arraylake_token,
        **{"header.X-Iceberg-Access-Delegation": "vended-credentials"},
    )


def sort_order() -> SortOrder:
    """Give the sort order member, lat, lon. A Hilbert layout has no Iceberg transform: unsorted."""
    if SORT == "hilbert":
        return SortOrder()
    return SortOrder(*(SortField(source_id=i, transform=IdentityTransform()) for i in _SORT_SOURCE_IDS))


def _check_schema(table: Table, slugs: Sequence[str]) -> Table:
    """Reject a table whose columns differ from the slug list, or whose declared row order is not ours.
    A silent append would mix layouts under one SortOrder claim."""
    declared = table.properties.get("wxtco.sort", "member")
    if declared != SORT:
        raise ValueError(f"table {TABLE_ID} is sorted {declared!r}; this writer is {SORT!r}")
    want = {f.name for f in iceberg_schema(slugs).fields}
    got = {f.name for f in table.schema().fields}
    if want != got:
        missing = sorted(want - got)
        extra = sorted(got - want)
        raise ValueError(f"table {TABLE_ID} schema mismatch: missing={missing} extra={extra}")
    return table


def ensure_table(catalog: Catalog, slugs: Sequence[str]) -> Table:
    """Create the namespace and table if absent. The slug list fixes the schema for the study."""
    try:
        catalog.create_namespace(NAMESPACE)
    except NamespaceAlreadyExistsError:
        pass
    try:
        return _check_schema(catalog.load_table(TABLE_ID), slugs)
    except NoSuchTableError:
        pass
    try:
        return catalog.create_table(
            TABLE_ID,
            schema=iceberg_schema(slugs),
            partition_spec=partition_spec(),
            sort_order=sort_order(),
            properties=TABLE_PROPERTIES,
        )
    except TableAlreadyExistsError:
        # Another writer won the race. Its table must still match our schema.
        return _check_schema(catalog.load_table(TABLE_ID), slugs)
