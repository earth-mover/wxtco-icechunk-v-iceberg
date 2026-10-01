"""Met Office key layout: <YYYY>/<MM>/<DD>/<THHMMZ>/<valid>-PT<hhhh>H<mm>M-<slug>.nc."""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from itertools import groupby

import obstore as obs
from obstore.store import ObjectStore

KEY_RE = re.compile(r"^(?P<valid>\d{8}T\d{4}Z)-PT(?P<hours>\d{4})H(?P<minutes>\d{2})M-(?P<slug>.+)\.nc$")
WINDOW_RE = re.compile(r"-(PT\d{2}H)$")
CYCLE_RE = re.compile(r"^(\d{4})/(\d{2})/(\d{2})/T(\d{2})(\d{2})Z$")


@dataclass(frozen=True)
class SourceFile:
    key: str
    slug: str
    lead_minutes: int
    valid_time: datetime


@dataclass(frozen=True)
class Diagnostic:
    slug: str
    files: tuple[SourceFile, ...]

    @property
    def is_level(self) -> bool:
        """Remove the window suffix first. A windowed level slug is still a level diagnostic."""
        s = WINDOW_RE.sub("", self.slug)
        return "_on_" in s and s.endswith("_levels")

    @property
    def window(self) -> str:
        m = WINDOW_RE.search(self.slug)
        return m[1] if m else ""


def parse_key(key: str) -> SourceFile | None:
    """Parse one object key. Return None for keys that are not forecast files."""
    m = KEY_RE.match(key.rsplit("/", 1)[-1])
    if m is None:
        return None
    valid = datetime.strptime(m["valid"], "%Y%m%dT%H%MZ").replace(tzinfo=UTC)
    return SourceFile(key, m["slug"], int(m["hours"]) * 60 + int(m["minutes"]), valid)


def cycle_time(cycle: str) -> datetime:
    m = CYCLE_RE.match(cycle)
    if m is None:
        raise ValueError(f"bad cycle: {cycle}")
    y, mo, d, h, mi = (int(x) for x in m.groups())
    return datetime(y, mo, d, h, mi, tzinfo=UTC)


def cycle_id(cycle: str) -> str:
    return cycle_time(cycle).strftime("%Y%m%dT%H%MZ")


def _child_prefixes(store: ObjectStore, prefix: str) -> list[str]:
    listing = obs.list_with_delimiter(store, prefix=prefix)
    return sorted(p.rstrip("/").rsplit("/", 1)[-1] for p in listing["common_prefixes"])


def _date_prefixes(store: ObjectStore, prefix: str) -> list[str]:
    """Date children only. A bucket root also holds control prefixes such as _progress/."""
    return [p for p in _child_prefixes(store, prefix) if p.isdigit()]


def available_cycles(store: ObjectStore) -> list[str]:
    """Every cycle under the store root, oldest first. Keys are listed, never constructed."""
    out: list[str] = []
    for y in _date_prefixes(store, ""):
        for mo in _date_prefixes(store, f"{y}/"):
            for d in _date_prefixes(store, f"{y}/{mo}/"):
                out.extend(f"{y}/{mo}/{d}/{t}" for t in _child_prefixes(store, f"{y}/{mo}/{d}/"))
    return sorted((c for c in out if CYCLE_RE.match(c)), key=cycle_time)


def list_cycle(store: ObjectStore, cycle: str) -> list[Diagnostic]:
    """All forecast files of one cycle grouped by diagnostic, sorted by slug then lead."""
    prefix = f"{cycle.rstrip('/')}/"
    files = sorted(
        filter(None, (parse_key(item["path"]) for page in obs.list(store, prefix=prefix) for item in page)),
        key=lambda f: (f.slug, f.lead_minutes),
    )
    return [Diagnostic(slug, tuple(g)) for slug, g in groupby(files, key=lambda f: f.slug)]


def surface_only(diagnostics: Sequence[Diagnostic]) -> list[Diagnostic]:
    return [d for d in diagnostics if not d.is_level]
