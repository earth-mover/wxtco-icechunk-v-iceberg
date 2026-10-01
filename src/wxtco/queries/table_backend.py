"""DuckDB over the wide Iceberg table."""

from __future__ import annotations

import os
import re
from collections.abc import Sequence
from typing import Any

import duckdb
import numpy as np
import xarray as xr

from wxtco.config import Settings, split_url
from wxtco.duck import _wheel_extension, connect_iceberg  # wheel lookup is shared, not public
from wxtco.ingest.table_schema import TABLE_ID, column_name
from wxtco.queries.base import Box, Point
from wxtco.source import cycle_time

# An Arraylake org becomes part of an ATTACH statement, which takes no parameter.
_ORG_RE = re.compile(r"^[A-Za-z0-9_-]+$")


def _load_httpfs(con: duckdb.DuckDBPyConnection) -> None:
    """Load httpfs, which REST catalogs need. It is neither built in nor autoloadable in duckdb 1.5."""
    path = _wheel_extension("duckdb_extension_httpfs")
    if path is not None:
        try:
            con.execute(f"LOAD '{path}'")
            return
        # A wheel built for another duckdb version fails to load. Fall back to the network.
        except duckdb.Error:
            pass
    con.execute("INSTALL httpfs; LOAD httpfs;")


# Lead used for grid discovery. Every cycle has lead 0.
GRID_LEAD_HOURS = 0


class TableBackend:
    """Read the wide Iceberg table with DuckDB. `table` is any scannable table expression."""

    name = "table"

    def __init__(self, con: duckdb.DuckDBPyConnection, table: str) -> None:
        self.con = con
        self.table = table
        # WXTCO_DUCKDB_FILE_CACHE=0 disables DuckDB's in-memory cache of fetched byte ranges (default on
        # since 1.3), so repeated runs measure S3 reads and not RAM.
        if os.environ.get("WXTCO_DUCKDB_FILE_CACHE", "1") in ("0", "false", "off"):
            con.execute("SET enable_external_file_cache=false")
        # Grid coordinates per cycle. Q1 runs 10,000 times a day and must not re-read them.
        self._grids: dict[str, tuple[np.ndarray, np.ndarray]] = {}

    @classmethod
    def local(cls, metadata_location: str) -> TableBackend:
        """Give a backend that scans one Iceberg metadata file directly."""
        return cls(connect_iceberg(), f"iceberg_scan('{metadata_location}')")

    @classmethod
    def arraylake(cls, settings: Settings) -> TableBackend:
        """Give a backend on the Arraylake REST catalog. Needs a token; no test covers this path."""
        if settings.arraylake_token is None:
            raise ValueError("ARRAYLAKE_TOKEN is required for the Arraylake table backend")
        if not _ORG_RE.match(settings.arraylake_org):
            raise ValueError(f"bad Arraylake org name: {settings.arraylake_org!r}")
        con = connect_iceberg()
        _load_httpfs(con)
        # Bind the secret. The token must never reach the SQL text.
        con.execute(
            "CREATE SECRET al (TYPE ICEBERG, TOKEN ?, ENDPOINT ?)",
            [settings.arraylake_token, settings.iceberg_uri],
        )
        # Vended S3 credentials expire after ~30 min and DuckDB never refreshes them (ExpiredToken
        # mid-scan on 2026-09-19). Read data files with the ambient AWS credential chain instead; the
        # catalog is used for metadata only.
        con.execute(
            "CREATE SECRET s3chain (TYPE S3, PROVIDER credential_chain, REGION ?, SCOPE ?)",
            [settings.copy_region, f"s3://{split_url(settings.copy_url)[0]}/"],
        )
        con.execute(
            f"ATTACH '{settings.arraylake_org}' AS wh (TYPE iceberg, SECRET al, ACCESS_DELEGATION_MODE 'none')"
        )
        return cls(con, f"wh.{TABLE_ID}")

    @staticmethod
    def _init(cycle: str) -> str:
        """Give the cycle init time as an SQL timestamp literal body."""
        return cycle_time(cycle).strftime("%Y-%m-%d %H:%M:%S")

    def _one(self, sql: str) -> tuple[Any, ...]:
        """Run `sql` and give its single row. Raise if the query gives no row."""
        row = self.con.execute(sql).fetchone()
        if row is None:
            raise ValueError(f"query gave no row: {sql}")
        return row

    # The query methods below interpolate only numbers, or values from `cycle_time`,
    # `column_name` and `TABLE_ID`. Never interpolate a raw caller string.

    def _grid(self, cycle: str) -> tuple[np.ndarray, np.ndarray]:
        """Give the cached (lat, lon) coordinate vectors of `cycle`. One query each on a miss.
        The grid is fixed, so one (init_time, lead_hours) partition is enough: 4 files, not a whole cycle."""
        if cycle not in self._grids:
            init = self._init(cycle)
            axes = []
            for axis in ("lat", "lon"):
                rows = self.con.execute(
                    f"select distinct {axis} from {self.table} "
                    f"where init_time = TIMESTAMP '{init}' and lead_hours = {GRID_LEAD_HOURS} "
                    f"and member = 0 order by {axis}"
                ).fetchall()
                if not rows:
                    raise ValueError(f"no rows for cycle {cycle}")
                axes.append(np.array([r[0] for r in rows], dtype="float32"))
            self._grids[cycle] = (axes[0], axes[1])
        return self._grids[cycle]

    def _nearest_cell(self, point: Point, cycle: str) -> tuple[float, float]:
        """Snap to the grid cell nearest `point` by Euclidean distance in degrees.

        Ties go to the first grid index, so bench points must be grid centers to agree across backends.
        """
        lats, lons = self._grid(cycle)
        d2 = (lats[:, None] - point.lat) ** 2 + (lons[None, :] - point.lon) ** 2
        iy, ix = np.unravel_index(int(np.argmin(d2)), d2.shape)
        return float(lats[iy]), float(lons[ix])

    def point_series(self, var: str, point: Point, cycle: str) -> xr.DataArray:
        """Give one variable at the nearest cell, dims (member, lead), with a `valid_time` coord."""
        lat, lon = self._nearest_cell(point, cycle)
        col = column_name(var)
        # `float(...)` of a stored float32 is exact, and `::FLOAT` narrows it back, so `=` is safe here.
        df = self.con.execute(
            f"select member, lead_hours, valid_time, {col} as value from {self.table} "
            f"where init_time = TIMESTAMP '{self._init(cycle)}' "
            f"and lat = {lat}::FLOAT and lon = {lon}::FLOAT order by member, lead_hours"
        ).df()
        wide = df.pivot(index="member", columns="lead_hours", values="value")
        valid = df.drop_duplicates("lead_hours").set_index("lead_hours")["valid_time"].loc[wide.columns]
        return xr.DataArray(
            wide.to_numpy(dtype="float32"),
            dims=("member", "lead"),
            coords={
                "member": wide.index.to_numpy(),
                "lead": wide.columns.to_numpy(),
                "valid_time": ("lead", valid.to_numpy(dtype="datetime64[ns]")),
            },
            name=var,
        )

    def box_fields(self, vars: Sequence[str], box: Box, cycle: str) -> xr.Dataset:
        """Give `vars` inside `box`, dims (member, lead, lat, lon).

        The pandas path materializes member x lead x cell rows. Use it for regional boxes
        of about 10^4 cells, not for global fields.
        """
        cols = ", ".join(column_name(v) for v in vars)
        df = self.con.execute(
            f"select member, lead_hours, lat, lon, {cols} from {self.table} "
            f"where init_time = TIMESTAMP '{self._init(cycle)}' "
            f"and lat between {box.lat_min} and {box.lat_max} "
            f"and lon between {box.lon_min} and {box.lon_max}"
        ).df()
        idx = df.set_index(["member", "lead_hours", "lat", "lon"]).sort_index()
        ds = xr.Dataset.from_dataframe(idx).rename({"lead_hours": "lead"})
        ds = ds.rename({column_name(v): v for v in vars if column_name(v) != v})
        return ds.astype("float32").sortby(["lat", "lon"])

    def lead_fields(self, vars: Sequence[str], cycle: str, lead_hours: int) -> np.ndarray:
        """Give one lead as float32 of shape (len(vars), member, ny, nx), rows ordered member, lat, lon."""
        cols = ", ".join(column_name(v) for v in vars)
        where = f"where init_time = TIMESTAMP '{self._init(cycle)}' and lead_hours = {lead_hours}"
        tbl = self.con.execute(
            f"select {cols} from {self.table} {where} order by member, lat, lon"
        ).to_arrow_table()
        shape = self._one(
            f"select count(distinct member), count(distinct lat), count(distinct lon) "
            f"from {self.table} {where}"
        )
        n_member, ny, nx = (int(v) for v in shape)
        want = n_member * ny * nx
        # A partial or absent lead would silently reshape into nonsense.
        if want == 0 or tbl.num_rows != want:
            raise ValueError(
                f"cycle {cycle} lead {lead_hours}: got {tbl.num_rows} rows, "
                f"expected {want} = {n_member} members x {ny} lat x {nx} lon"
            )
        out = np.empty((len(vars), n_member, ny, nx), dtype="float32")
        for i, v in enumerate(vars):
            out[i] = tbl[column_name(v)].to_numpy(zero_copy_only=False).reshape(n_member, ny, nx)
        return out

    def batch_fields(self, vars: Sequence[str], cycles: Sequence[str], leads: Sequence[int]) -> np.ndarray:
        """Give all samples as float32 (cycle, lead, var, member, ny, nx) from one select.

        One count query on the first (cycle, lead) gives the member and grid sizes.
        Rows sort by init, lead, member, lat, lon; the cycle and lead axes then follow the caller order.
        """
        if not cycles or not leads:
            raise ValueError("no (cycle, lead) pairs")
        # Sorted unique axes match the SQL row order; duplicates in the caller order are allowed.
        ucycles = sorted(set(cycles), key=cycle_time)
        uleads = sorted({int(x) for x in leads})
        cols = ", ".join(column_name(v) for v in vars)
        inits = ", ".join(f"TIMESTAMP '{self._init(c)}'" for c in ucycles)
        lead_list = ", ".join(str(x) for x in uleads)
        tbl = self.con.execute(
            f"select {cols} from {self.table} "
            f"where init_time in ({inits}) and lead_hours in ({lead_list}) "
            f"order by init_time, lead_hours, member, lat, lon"
        ).to_arrow_table()
        shape = self._one(
            f"select count(distinct member), count(distinct lat), count(distinct lon) from {self.table} "
            f"where init_time = TIMESTAMP '{self._init(cycles[0])}' and lead_hours = {int(leads[0])}"
        )
        n_member, ny, nx = (int(v) for v in shape)
        nc, nl = len(ucycles), len(uleads)
        want = nc * nl * n_member * ny * nx
        # A partial or absent cycle or lead would silently reshape into nonsense.
        if want == 0 or tbl.num_rows != want:
            raise ValueError(
                f"cycles {', '.join(ucycles)} leads {uleads}: got {tbl.num_rows} rows, expected {want} = "
                f"{nc} cycles x {nl} leads x {n_member} members x {ny} lat x {nx} lon"
            )
        out = np.empty((nc, nl, len(vars), n_member, ny, nx), dtype="float32")
        for i, v in enumerate(vars):
            col = tbl[column_name(v)].to_numpy(zero_copy_only=False)
            out[:, :, i] = col.reshape(nc, nl, n_member, ny, nx)
        ci = [ucycles.index(c) for c in cycles]
        li = [uleads.index(int(x)) for x in leads]
        if ci != list(range(nc)):
            out = out[ci]
        if li != list(range(nl)):
            out = out[:, li]
        return out

    def bytes_read(self) -> int | None:
        """Give None. DuckDB does not report bytes read per query."""
        return None
