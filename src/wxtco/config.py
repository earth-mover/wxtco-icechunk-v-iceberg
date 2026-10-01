"""Project settings. Values come from the environment, then `.env`, then defaults."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv
from obstore.store import LocalStore, ObjectStore, S3Store

SOURCE_URL = "s3://met-office-global-ensemble-model-data/global-ensemble"
SOURCE_REGION = "eu-west-2"
COPY_URL = "s3://em-tco-mogreps/netcdf"
COPY_REGION = "us-east-1"
ICEBERG_URI = "https://api.earthmover.io/iceberg"


@dataclass(frozen=True)
class Settings:
    source_url: str
    source_region: str
    copy_url: str
    copy_region: str
    arraylake_org: str
    arraylake_token: str | None
    iceberg_uri: str
    # Informational only. obstore reads AWS_ACCESS_KEY_ID style variables, not profiles.
    aws_profile: str | None

    @classmethod
    def from_env(cls) -> Settings:
        """Read settings. `.env` in the CWD fills gaps but never overrides the environment."""
        load_dotenv(Path.cwd() / ".env", override=False)
        return cls(
            source_url=os.environ.get("WXTCO_SOURCE_URL", SOURCE_URL),
            source_region=os.environ.get("WXTCO_SOURCE_REGION", SOURCE_REGION),
            copy_url=os.environ.get("WXTCO_COPY_URL", COPY_URL),
            copy_region=os.environ.get("WXTCO_COPY_REGION", COPY_REGION),
            arraylake_org=os.environ.get("WXTCO_ORG") or os.environ.get("ARRAYLAKE_ORG", ""),
            arraylake_token=os.environ.get("ARRAYLAKE_TOKEN"),
            iceberg_uri=os.environ.get("WXTCO_ICEBERG_URI", ICEBERG_URI),
            aws_profile=os.environ.get("AWS_PROFILE"),
        )


def _parse_url(url: str) -> tuple[str, str, str]:
    """Parse a supported url into (scheme, bucket-or-path, prefix)."""
    scheme, sep, rest = url.partition("://")
    if not sep:
        raise ValueError(f"unsupported url scheme: {url}")
    if scheme == "file":
        if not rest.startswith("/"):
            raise ValueError(f"file url needs an absolute path: {url}")
        return scheme, rest, ""
    if scheme != "s3":
        raise ValueError(f"unsupported url scheme: {url}")
    bucket, _, prefix = rest.partition("/")
    if not bucket:
        raise ValueError(f"s3 url needs a bucket: {url}")
    return scheme, bucket, prefix.strip("/")


def split_url(url: str) -> tuple[str, str]:
    """Split `s3://bucket/prefix` into (bucket, prefix) and `file:///path` into (path, "")."""
    _, root, prefix = _parse_url(url)
    return root, prefix


def store_for(url: str, region: str | None = None, anonymous: bool = False) -> ObjectStore:
    """Return an obstore store rooted at `url`. Keys are relative to the prefix. Create a local root if absent."""
    scheme, root, prefix = _parse_url(url)
    if scheme == "file":
        Path(root).mkdir(parents=True, exist_ok=True)
        return LocalStore(root)
    # obstore rejects region=None, so send the key only when it has a value.
    config: dict[str, str | bool] = {"skip_signature": anonymous}
    if region is not None:
        config["region"] = region
    return S3Store(root, prefix=prefix or None, **config)
