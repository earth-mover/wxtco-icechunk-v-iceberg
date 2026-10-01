# 015 NetCDF-direct methods: Download and FUSE

**Status:** DECIDED 2026-09-20 (Ryan). Two new top-level methods with no ETL by construction.

## Context

Stage A compared two transformed layouts (Iceberg table, virtual Icechunk). Ryan asked for two
methods that use the copied NetCDF files as they are, so the TCO includes the "do nothing" baseline.

## Decision

- **Download**: per query, list the cycle, download every needed object in parallel to a fresh
  temp dir, read with h5netcdf, delete the dir. No cache of any kind between queries. Two transfer
  clients, benchmarked side by side under separate method names: `download` (obstore, one GET per
  object, asyncio semaphore) and `download_crt` (boto3's CRT S3 Transfer Manager, ranged GETs,
  bandwidth-paced). The faster one is the method's number.
- **FUSE**: read the files in place through a read-only Mountpoint for Amazon S3 mount of the copy
  bucket on the EC2 job node (`WXTCO_FUSE=1` at launch; `docs/infra/fuse-research.md`). h5netcdf
  reads only the HDF5 chunks a query touches. After each query the backend evicts the files it read
  from the page cache (`posix_fadvise DONTNEED`), so repeated runs read S3 again.
- **Mountpoint settings**: bucket root mounted at `/mnt/mogreps` (path == key), `--read-only`,
  no `--cache`, `--max-threads 64`, `--metadata-ttl indefinite` (default in `launch.sh`; the copy is
  immutable, and `minimal` would add a HEAD and a LIST per file open, about 8x the request cost).
  `WXTCO_FUSE_TTL=minimal` measures the revalidating variant.
- **Cost model rows**: both methods own no bytes (`data` = 0) and carry the NetCDF copy under
  `with_netcdf` (6494.0 GB). No ETL rows. Query cost from the bench CSVs.

## Consequences

- Q1 through either method moves a whole variable for a cycle (171 files, about 2.8 GB) to read 18 x 171
  values. That is the method, not a bug; the bench measures it as designed.
- FUSE request count per HDF5 open is not yet measured; the first FUSE benchmark records S3 request
  metrics for one controlled run (`docs/infra/fuse-research.md`, section 7).
- Benchmarks run on EC2 (bandwidth, instance role). Both instance sizes used elsewhere (m7i.4xlarge,
  c7i.16xlarge) are run, because network bandwidth is the lever for these methods.
