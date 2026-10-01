import obstore as obs
from obstore.store import MemoryStore

from wxtco.copy import copy_commands, verify_copy


def test_copy_commands():
    cmds = copy_commands(
        "s3://src/global-ensemble", "s3://dst/netcdf", ["2026/09/16/T0000Z"], "eu-west-2", "us-east-1"
    )
    assert len(cmds) == 1
    c = cmds[0]
    assert "s3://src/global-ensemble/2026/09/16/T0000Z/ s3://dst/netcdf/2026/09/16/T0000Z/" in c
    assert "--exclude '*_on_*_levels.nc'" in c
    assert "--source-region eu-west-2" in c and "--region us-east-1" in c


def test_copy_commands_strips_trailing_slashes():
    cmds = copy_commands(
        "s3://src/global-ensemble/", "s3://dst/netcdf/", ["2026/09/16/T0000Z"], "eu-west-2", "us-east-1"
    )
    assert "//" not in cmds[0].replace("s3://", "")
    assert "s3://src/global-ensemble/2026/09/16/T0000Z/ s3://dst/netcdf/2026/09/16/T0000Z/" in cmds[0]


def test_verify_copy_reports_missing():
    src, dst = MemoryStore(), MemoryStore()
    keys = [
        "2026/09/16/T0000Z/20260916T0000Z-PT0000H00M-a.nc",
        "2026/09/16/T0000Z/20260916T0100Z-PT0001H00M-a.nc",
        "2026/09/16/T0000Z/20260916T0000Z-PT0000H00M-x_on_height_levels.nc",
    ]
    for k in keys:
        obs.put(src, k, b"1234")
    obs.put(dst, keys[0], b"1234")
    # Objects outside the wanted surface set must not add to the byte count.
    obs.put(dst, keys[2], b"12345678")
    obs.put(dst, "2026/09/16/T0000Z/junk.txt", b"abcdefgh")
    r = verify_copy(src, dst, "2026/09/16/T0000Z")
    assert r.expected == 2 and r.present == 1
    assert r.missing == (keys[1],)
    assert r.bytes == 4
