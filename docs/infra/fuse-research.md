# FUSE query method: mounting `s3://em-tco-mogreps/netcdf/` on the job nodes

Research only. Nothing here was run against AWS. Facts about Mountpoint come from its source and
docs at commit `fd69056` (2026-09-18, v1.24.0 + unreleased), read from a local clone because
`github.com`, `docs.aws.amazon.com` and `aws.amazon.com` are all blocked by this session's proxy.

## Recommendation

**Use AWS Mountpoint for Amazon S3 (`mount-s3`) v1.2x, read-only, no data cache, on the EC2 job
nodes. Do not use s3fs-fuse or goofys.**

Why:

- It is the only client of the three built on the AWS CRT, which does the same parallel ranged-GET
  fan-out the rest of our stack already relies on, and it auto-sizes its request concurrency to the
  instance's advertised network bandwidth. Published comparisons put it at roughly 6-8x s3fs on
  sequential read. goofys hard-codes 5 connections and a 20 MB chunk, which is wrong for 16 MB
  HDF5 files read in scattered pieces.
- It is supported by AWS, packaged for AL2023 (`dnf install mount-s3`), and needs no custom AMI.
- It is read-optimised and explicitly supports random reads and `lseek`; the write limitations that
  make it a poor general-purpose file system are irrelevant to us because the mount is `--read-only`.
- Caching is opt-in. With no `--cache`/`--cache-xz` flag it stores no object data on disk, which is
  what an honest S3-read benchmark needs.

Three things to decide before the first benchmark run:

1. **Mount from `user_data`, not from a `run_job.sh` SSM command.** `mount-s3` forks but never calls
   `setsid` (`mountpoint-s3/src/run.rs`), so the daemon stays in the caller's process group. SSM Run
   Command kills that group when the document returns — see the DEVIATION note in
   `scripts/ec2/run_job.sh` — which would tear the mount down under a running job. Section 6 has the
   snippet.
2. **Decide what `--metadata-ttl` means for the numbers.** The default (`minimal`) revalidates every
   path lookup and every `open()` against S3, and each lookup is one `HeadObject` **plus** one
   `ListObjectsV2`. `ListObjectsV2` bills at the PUT/LIST rate, 12.5x a GET. At our Q1 rate that is
   ~$640/month of pure request cost against ~$82/month with `--metadata-ttl indefinite` (section 7).
   Our copy bucket is immutable for the study, so `indefinite` is defensible — but it must be stated
   as a tuning choice, not smuggled in.
3. **The Linux page cache will make the second read of a file free.** Mountpoint only asks for
   `FOPEN_DIRECT_IO` when the *application* opened with `O_DIRECT`, and the h5py wheel's HDF5 2.0.0
   has no `direct` VFD, so xarray cannot do that. Drop the caches between runs (section 4).

## 1. Candidate clients

| client | engine | fit here |
|---|---|---|
| **Mountpoint for S3 (`mount-s3`)** | Rust + AWS CRT, libfuse2 | **Recommended.** Read-optimised, parallel ranged GETs, prefetch window that adapts to the access pattern, bandwidth auto-detected on EC2, AL2023 package, AWS-supported. Its weaknesses (no partial writes, no random writes, no hard links, no POSIX lock forwarding) are all write-side or irrelevant under `--read-only`. |
| s3fs-fuse | C++ + libcurl | Mature and more POSIX-complete, but slower: roughly 1/6 to 1/8 of Mountpoint on FIO sequential read in third-party comparisons. Its POSIX completeness buys us nothing for read-only HDF5. |
| goofys | Go | Fast for streaming, but "weak POSIX compatibility" by its own README, and its read path is hard-coded to 5 connections x 20 MB chunks, which cannot be tuned to a 16 MB file read in scattered pieces. Effectively unmaintained. |
| rclone mount / juicefs / s3backer | various | rclone mount is a convenience tool, not a throughput tool. JuiceFS needs an external metadata store and rewrites the data layout, so it is a different storage method, not a view of our existing objects. s3backer presents a block device, not our keys. None belong in this comparison. |
| goofys/s3fs successors (Storj, SeaweedFS gateways, Fusion) | various | Vendor stacks aimed at other object stores or at caching. Out of scope. |

Also worth naming for the record but *not* FUSE: `obstore` (already the `DownloadBackend` in
`src/wxtco/queries/netcdf_backend.py`) and `s3fs`+`fsspec` file-like objects. The whole point of the
FUSE method is to measure what it costs to keep the application unmodified.

## 2. Kernel and FUSE requirements

**On AL2023 EC2: nothing to do, and no custom AMI.**

- FUSE is a stock kernel feature (`fuse` and `fuseblk` in `/proc/filesystems`); the module
  auto-loads on the first `mount -t fuse`, and `/dev/fuse` is created by the kernel's device
  infrastructure. AWS's own documented `user_data` example installs the `mount-s3` rpm on a plain
  AL2023 x86 AMI and mounts, with no AMI change and no kernel work — that is documentation-grade
  evidence that our `al2023-ami-kernel-default-x86_64` (the AMI `scripts/ec2/launch.sh` resolves
  from SSM) is sufficient.
- Mountpoint links **libfuse v2**, not fuse3 (`doc/INSTALL.md`: "install the FUSE and libfuse (v2)
  packages"). The rpm declares that dependency, so `dnf` pulls `fuse`/`fuse-libs` from the AL2023
  repo if they are not already present. Do not install fuse3 expecting it to satisfy this.
- The `fusermount` setuid helper is only needed for two things: mounting as a non-root user, and
  `--auto-unmount`. Our jobs run as root under SSM, and `mountpoint-s3-fuser`'s "pure" path calls
  `mount(2)` directly and only falls back to `fusermount` if that fails
  (`mountpoint-s3-fuser/src/mnt/fuse_pure.rs`). Avoid `--auto-unmount`, which forces the helper path.
- `--allow-other` is only needed if the reader process runs as a different user from the mounter;
  it additionally needs `user_allow_other` in `/etc/fuse.conf`. Mounting and reading both as root
  avoids all of it.

**The cloud session container: the stated premise is wrong, but the conclusion still holds.**

The task brief says a FUSE mount is impossible in our cloud session container because there is no
`/dev/fuse` and no `CAP_SYS_ADMIN`. Measured in this session (2026-09-20), both are present, and a
FUSE mount actually succeeds:

    $ ls -l /dev/fuse
    crw------- 1 root root 10, 229 /dev/fuse
    $ grep CapEff /proc/self/status          # 0x1fffeffffff, bit 21 (CAP_SYS_ADMIN) set
    CapEff: 000001fffeffffff
    $ cat /proc/self/uid_map                 # initial user namespace, real root
             0          0 4294967295
    $ grep fuse /proc/filesystems
    nodev   fuse
    # open("/dev/fuse", O_RDWR) then mount("fusetest", tmpdir, "fuse", 0,
    #   "fd=3,rootmode=40000,user_id=0,group_id=0")  ->  0        (umount2 -> 0)

So the honest statement is: *mounting works here; running the benchmark here still does not, for
other reasons.*

- The session's egress is an authenticating proxy with a private CA. Mountpoint's CRT does honour
  `HTTP_PROXY`/`HTTPS_PROXY`/`NO_PROXY` (added in v1.20, `mountpoint-s3/CHANGELOG.md`) and
  `--ca-bundle`/`AWS_CA_BUNDLE`, so it is *plausible* rather than impossible — but untested, and a
  proxied path is not what we are trying to measure.
- The session holds only the `wxtco-controller` IAM user key, not the `wxtco-ec2` instance role
  (`docs/infra/cloud-session.md`), and decision 014 keeps data movement off the controller.
- 4 vCPU / 16 GB behind a proxy is not a bandwidth-representative node. Benchmarks belong on EC2
  regardless of whether the mount would come up.

Caveat: this is one observation of one cloud-session container (a Firecracker microVM,
`systemd-detect-virt` reports `docker`) on one date. Other runtimes — gVisor sandboxes, Fargate,
unprivileged Docker — genuinely lack `/dev/fuse` or `CAP_SYS_ADMIN`, and AWS's own guidance is that
running Mountpoint in a container "requires giving the container broad root-level privileges to your
host system". Do not generalise this measurement into a claim that containers can always mount.

## 3. Install, mount and unmount on AL2023

Install, preferring the distro package and falling back to the rpm:

    # AL2023 ships mount-s3 since 2023.9.20251110; the rpm is the fallback for older AMIs.
    dnf install -y mount-s3 || {
      curl -fsSL -o /tmp/mount-s3.rpm https://s3.amazonaws.com/mountpoint-s3-release/latest/x86_64/mount-s3.rpm
      dnf install -y /tmp/mount-s3.rpm
    }
    mount-s3 --version

Mount, read-only, no data cache, instance-role credentials:

    mkdir -p /mnt/mogreps
    mount-s3 em-tco-mogreps /mnt/mogreps \
      --read-only \
      --region us-east-1 \
      --max-threads 64 \
      --metadata-ttl minimal          # or `indefinite`; see section 7 before choosing

Notes on each part:

- **Credentials.** Nothing to pass. Mountpoint uses the standard AWS chain and picks up the
  `wxtco-ec2` instance profile through IMDS on its own. That role already grants `s3:GetObject` and
  `s3:ListBucket` on this bucket (`scripts/aws/iam_policy_wxtco.json`), which is exactly what a
  read-only mount needs. Do not set `AWS_ACCESS_KEY_ID` in the job environment, or it wins over the
  role.
- **`--region us-east-1`.** Region is normally auto-detected; passing it removes a startup IMDS/S3
  round trip and one failure mode.
- **Mount the whole bucket, not `--prefix netcdf/`.** With the bucket root mounted, the path is the
  key: `/mnt/mogreps/netcdf/2026/09/12/T0000Z/<file>.nc`. That is exactly what
  `FuseBackend._materialize` already builds (`self.mount_root / f.key`), so no code changes. The
  alternative, `mount-s3 s3://em-tco-mogreps/netcdf/ /mnt/mogreps`, strips the prefix and would make
  the mount path `/mnt/mogreps/2026/...`, breaking that mapping.
- **No `--cache`, no `--cache-xz`.** Absence is how you get "no data cache"; there is no
  `--no-cache` flag.
- **`--allow-other` is not needed** as long as the same user mounts and reads.

Unmount:

    umount /mnt/mogreps            # root; the daemon exits when the mount goes away
    # non-root, or if the mount is wedged:
    fusermount -u /mnt/mogreps
    umount -l /mnt/mogreps         # last resort: lazy detach

Sanity checks after mounting (all cheap, none write):

    mount | grep mountpoint-s3                       # expect ro,nosuid,nodev,noatime,default_permissions
    ls /mnt/mogreps/netcdf/2026/09/12/T0000Z | head
    python -c "import xarray as xr; print(xr.open_dataset('<path>', engine='h5netcdf'))"

## 4. Caching: what to turn off, and what you cannot turn off

**Mountpoint's own data cache: off by default.** Confirmed in `doc/CONFIGURATION.md` — object
content is cached only when `--cache <DIR>` (local disk or tmpfs) or `--cache-xz <BUCKET>` (S3
Express One Zone) is given. Omit both and every application read that misses the prefetch window
goes to S3.

**Mountpoint's metadata cache: shaped by `--metadata-ttl`.** From
`mountpoint-s3-fs/src/fs/config.rs`:

| setting | `serve_lookup_from_cache` | kernel file TTL | kernel dir TTL | negative cache |
|---|---|---|---|---|
| `minimal` (default without `--cache`) | false | 100 ms | 1000 ms | off |
| `<seconds>` | true | N s | N s | on, N s |
| `indefinite` | true | forever | forever | on, forever |
| default *with* `--cache`/`--cache-xz` | true | 60 s | 60 s | on, 60 s |

`minimal` is not zero: the kernel still caches attributes for 100 ms/1 s, because a zero TTL makes
Linux re-`getattr` every `readdir` entry. What `minimal` really guarantees is that `lookup` and
`open` always re-check S3 (`superblock.rs`: `force_revalidate_if_remote = !serve_lookup_from_cache
|| flags.direct_io()`). That is the honest setting for "no caching", and the expensive one.

**The Linux page cache is the one you have to fight.** Mountpoint does *not* set the `direct_io`
mount option; the only mount options it sets are `default_permissions`, `fsname`, `noatime` and
(with `--read-only`) `ro` (`mountpoint-s3-fs/src/fuse/config.rs`). It returns `FOPEN_DIRECT_IO` per
open only when the application passed `O_DIRECT` (`mountpoint-s3-fs/src/fs.rs:400`). So by default:

- Repeated reads of the same bytes **in one process** are served by the kernel from clean page-cache
  pages. Mountpoint never sees them and S3 is never called.
- The same is true **across processes**: the page cache is per-inode, not per-process, so a second
  reader process on the same node reads a warm file for free.
- Pages survive the reader process exiting. They are dropped on memory pressure, when the inode is
  evicted, or when Mountpoint invalidates the file (etag change on revalidation).

Ways to defeat it, in order of preference:

    sync; echo 3 > /proc/sys/vm/drop_caches     # between benchmark runs; needs root; global
    # or, per file, no root needed, from Python:
    #   fd = os.open(path, os.O_RDONLY); os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)

`O_DIRECT` is **not** available to us from Python: the `direct` VFD is absent from the HDF5 2.0.0
built into the h5py 3.16 wheel we pin (`h5py.File(p, driver="direct")` raises
`ValueError: Unknown driver type 'direct'`, verified locally). Even if it were, `O_DIRECT` on a
Mountpoint file also forces a metadata revalidation on every open, which would change the request
mix. Use `drop_caches` and say so in the results.

**A third cache, inside xarray.** `xr.open_dataset(..., engine="h5netcdf")` on a local path goes
through `CachingFileManager`, keyed on path plus open kwargs, so re-opening the same file in the
same process reuses the live h5py handle *and its HDF5 metadata cache* — no FUSE traffic at all.
Benchmarks that loop over the same cycle must close datasets and ideally run each repeat in a fresh
process.

## 5. Performance tuning for HDF5 random reads

**How Mountpoint reads.** Per file handle it opens one flow-controlled `GetObject` from the current
offset to EOF and grows a read window as the reader stays sequential
(`mountpoint-s3-fs/src/prefetch.rs`):

- initial request `1 MiB + 128 KiB` (`INITIAL_REQUEST_SIZE`), deliberately small to keep
  small-random-read latency down;
- window multiplier 2x per sequential read, up to 2 GiB per handle;
- the stream is split into parts of `--read-part-size`, default 8 MiB (8,388,608 B) — **each part is
  a separate billed ranged GET**;
- forward seek tolerance 16 MiB (the gap is downloaded and discarded, costing bytes, not requests);
  backward seek tolerance 1 MiB. **Any seek outside those windows cancels the request and starts a
  new `GetObject`**;
- `--max-threads` (default **16**) caps concurrent FUSE operations, and `--maximum-throughput-gbps`
  defaults to the instance's advertised bandwidth when running on EC2.

**Instance bandwidth.** c7i.16xlarge is 25 Gbps sustained; m7i.4xlarge is "up to 12.5 Gbps" with a
~6.25 Gbps baseline and credit-based burst (third-party spec sites; `docs.aws.amazon.com` is blocked
here — confirm with `aws ec2 describe-instance-types --query 'InstanceTypes[].NetworkInfo'` on the
node before quoting). The burst behaviour matters: a long FUSE benchmark on an m7i.4xlarge can
exhaust network credits mid-run and silently change the answer. The existing finding that four times
the cores and bandwidth bought only 5-8% on virtual queries (`docs/findings/notes.md`, 2026-09-19)
says the same thing it will say here: this workload is latency- and request-bound, not
bandwidth-bound.

**Settings to use:**

- `--max-threads 64` (or 128) whenever more than ~16 reader processes run concurrently. The default
  16 is a hard ceiling on in-flight FUSE ops and will quietly serialise a wide reader pool.
- Leave `--read-part-size` at 8 MiB. Raising it to 16 MiB would make a full-file read 1-2 GETs
  instead of 3, but AWS's own testing says 8 MiB is the largest value that still achieves maximum
  throughput, and our files are only 16 MB, so the saving is one GET per file against a real
  latency cost.
- Leave `--memory-target` alone (defaults to 95% of RAM) unless several `mount-s3` processes share
  a node.

**Pitfalls specific to HDF5 over FUSE:**

1. **Metadata is scattered, and every scatter can cost a request.** HDF5 reads the superblock, then
   object headers, B-tree/fixed-array chunk indices, then chunks. Each read that lands outside the
   1 MiB-backward/16 MiB-forward window restarts the `GetObject`. Our files are 16 MB, so most
   forward jumps fall inside the window — but they are paid for in transferred bytes, so a "small
   slice" can still pull a megabyte or more per file.
2. **Threads do not parallelise.** xarray's h5netcdf path takes the process-global `HDF5_LOCK` for
   reads, and h5py is not thread-safe. Concurrency must come from **processes**. Size `--max-threads`
   to the process count, not the thread count.
3. **`xr.open_dataset(path, engine="h5netcdf")` needs nothing special.** A Mountpoint path is an
   ordinary path. Two optional knobs, both verified to reach `h5py.File` (xarray merges
   `driver_kwds` into the kwargs it hands `h5netcdf.File`, which forwards `**kwargs` to h5py):

       # Chunk cache: only helps if one query re-reads the same chunk. It never helps the first read.
       ds = xr.open_dataset(p, engine="h5netcdf",
                            driver_kwds={"rdcc_nbytes": 64 << 20, "rdcc_nslots": 10007})

   Default `rdcc_nbytes` is 1 MiB / 521 slots. `page_buf_size` is useless here: it needs files
   written with paged aggregation, which the Met Office files are not.
4. **File locking is fine, but know the failure.** Mountpoint answers `getlk`/`setlk` with
   unsupported and does not negotiate `FUSE_POSIX_LOCKS`/`FUSE_FLOCK_LOCKS` at INIT
   (`mountpoint-s3-fs/src/fuse.rs`, `fs.rs:196`), so the kernel handles HDF5's lock locally and the
   open succeeds. If a future version changes that and opens fail with "unable to lock file", set
   `HDF5_USE_FILE_LOCKING=FALSE` or pass `locking=False`.
5. **No random writes — irrelevant.** The mount is `--read-only`; HDF5 never writes.
6. **`readdirplus` is on** (`fs.rs:196`), so a `readdir` returns attributes and primes the dentry
   cache. Opening files right after listing their directory can skip a lookup each — but only within
   the 100 ms file TTL under `--metadata-ttl minimal`, which is too short to help in practice.
7. **`FuseBackend._materialize` calls `path.exists()` per file.** Under `minimal` that is an extra
   `HeadObject` + `ListObjectsV2` per file unless it lands inside the 100 ms window. Consider
   dropping the check, or counting it honestly as part of the method's cost.

## 6. `user_data` snippet and how the backend finds files

Add to `scripts/ec2/user_data.sh` behind `${WXTCO_FUSE}`, substituted by `launch.sh` the same way
`${WXTCO_ORG}` and `${WXTCO_CODE_SHA}` already are (one more `-e "s/\${WXTCO_FUSE}/.../"` in the
`sed`, defaulting to `0`). Mount here, in cloud-init, **not** in an SSM job: `mount-s3` forks
without `setsid`, so an SSM-launched daemon dies with the command's process group.

```bash
# Optional read-only S3 mount for the FUSE query method. Off unless WXTCO_FUSE=1.
if [ "${WXTCO_FUSE}" = "1" ]; then
  # AL2023 carries mount-s3 since 2023.9.20251110; the rpm is the fallback.
  dnf install -y mount-s3 || {
    curl -fsSL -o /tmp/mount-s3.rpm https://s3.amazonaws.com/mountpoint-s3-release/latest/x86_64/mount-s3.rpm
    dnf install -y /tmp/mount-s3.rpm
  }
  mkdir -p /mnt/mogreps
  # fstab + systemd, so the mount outlives any one SSM command and survives a reboot.
  # No --cache: benchmarks must measure S3 reads. metadata-ttl is the honest-cost knob.
  echo "s3://em-tco-mogreps/ /mnt/mogreps mount-s3 _netdev,nosuid,nodev,nofail,ro,region=us-east-1,max-threads=64,metadata-ttl=minimal 0 0" >> /etc/fstab
  systemctl daemon-reload
  mount -a
  mountpoint -q /mnt/mogreps && echo ready > /opt/wxtco/FUSE_READY
fi
```

`_netdev,nosuid,nodev` are mandatory for fstab mounts; `nofail` keeps a mount failure from blocking
boot. Note the fstab option syntax is `key=value` without the leading `--`.

**Path mapping.** The bucket root is mounted, so the mount path is the S3 key verbatim:

    s3://em-tco-mogreps/netcdf/2026/09/12/T0000Z/<file>.nc
    /mnt/mogreps/netcdf/2026/09/12/T0000Z/<file>.nc

`FuseBackend(store, mount_root=Path("/mnt/mogreps"))` already does `self.mount_root / f.key` with
`f.key` carrying the `netcdf/` prefix, so the backend needs `WXTCO_FUSE_ROOT=/mnt/mogreps` and
nothing else. Listing still goes through `obstore` (`_diagnostics` → `list_cycle`), which is the
right call: a FUSE `readdir` of a 14,000-entry directory is 14 `ListObjectsV2` pages either way, and
keeping the listing in obstore keeps it comparable with the other backends.

## 7. Cost: requests per file, and what that scales to

Same-region S3→EC2 transfer is free, so the dollar cost of this method is **requests only**. Bytes
cost wall-clock, not money. Unit prices from `prices.toml` (us-east-1 list, 2026-09):
GET/HEAD `$0.0004/1000` = `$4.0e-7`; `ListObjectsV2` bills at the PUT/LIST rate `$0.005/1000` =
`$5.0e-6`, i.e. **12.5x a GET**.

**Requests per file.** Two kinds:

*Metadata.* Every uncached `lookup` is one `HeadObject` **plus** one `ListObjectsV2` (max-keys=1,
delimiter `/`) issued concurrently, because Mountpoint must decide whether the name is a file or a
shadowing implicit directory (`superblock.rs`, ~line 1530). Under `--metadata-ttl minimal`, a read
of one file in a directory already traversed costs about 2 such lookups: the kernel `LOOKUP` of the
file name (100 ms TTL) and the forced revalidation on `open`. The five directory components above it
amortise over the 1 s dir TTL.

*Data.* One flow-controlled `GetObject` split into `--read-part-size` (8 MiB) parts, each a billed
ranged GET, restarted on any out-of-window seek.

| case (per 16 MB file) | HEAD | LIST | GET | $ per file |
|---|---|---|---|---|
| full read, `--metadata-ttl minimal` | 2 | 2 | ~3 (1.125 MiB initial + 2 parts) | **$1.2e-5** |
| full read, `--metadata-ttl indefinite` (warm) | 0 | 0 | ~3 | **$1.2e-6** |
| small slice, `minimal` | 2 | 2 | 1-3 | **$1.1e-5** |
| small slice, `indefinite` (warm) | 0 | 0 | 1-3 | **$4e-7 - 1.2e-6** |

Two things fall out of that table:

1. **Under `minimal`, ~83% of the cost is `ListObjectsV2`, not data.** The metadata protocol, not
   the reads, sets the price.
2. **A small slice costs almost as much as a full file read.** Request count is nearly independent
   of bytes, so the FUSE method has no "cheap small query" mode. This is the structural difference
   from the virtual/Icechunk method, where a point series issues one range request per chunk with no
   per-file metadata tax.

**At our scale** (`prices.toml [workload]`, and 171 leads per diagnostic per cycle from
`docs/findings/notes.md`):

| workload | `minimal` | `indefinite` |
|---|---|---|
| Q1, one point series = 171 files | $0.0021 | $0.00027 |
| Q1 at 10,000/day, per month | **~$640** | **~$82** |
| one full cycle scanned (11,521 surface files) | $0.14 | $0.014 |
| the whole 6.5 TB copy read once (~390,000 files) | ~$4.7 | ~$0.47 |

The Q1 line is the headline: at the decision-010 query rate, request cost alone under the default
metadata setting is the same order as the *entire* monthly TCO of the table method (634, stage A
summary). Whichever TTL we choose, state it next to the number — the 8x gap between the two rows is
a configuration choice, not a property of FUSE.

Add to this the per-query cycle listing if the backend keeps doing one (`_diagnostics` does: 11,521
keys / 1,000 per page = 12 `ListObjectsV2` = $6.0e-5 per query, ~$18/month at 10,000 Q1/day).

**Unverified / to measure on the node:**

- The ~3 GETs per full 16 MB read is derived from the prefetcher's constants, not observed. Measure
  it: `--otlp-endpoint`, or CloudTrail data events, or simply `s3:GetObject` counts from S3 server
  access logs for one controlled run.
- How many `GetObject` restarts a real Met Office HDF5 open causes. This is the single number that
  decides whether the FUSE method's request cost is "3 per file" or "15 per file", and it can only
  be answered by running it.

## Sources

Read directly (clone of `awslabs/mountpoint-s3` at `fd69056`, 2026-09-18; the GitHub web UI,
`docs.aws.amazon.com` and `aws.amazon.com` are blocked by this session's proxy):

- https://github.com/awslabs/mountpoint-s3/blob/main/doc/INSTALL.md
- https://github.com/awslabs/mountpoint-s3/blob/main/doc/CONFIGURATION.md
- https://github.com/awslabs/mountpoint-s3/blob/main/doc/SEMANTICS.md
- https://github.com/awslabs/mountpoint-s3/blob/main/doc/BENCHMARKING.md
- https://github.com/awslabs/mountpoint-s3/blob/main/README.md
- https://github.com/awslabs/mountpoint-s3/blob/main/mountpoint-s3/CHANGELOG.md
- `mountpoint-s3/src/cli.rs`, `mountpoint-s3/src/run.rs` (flag defaults; fork without setsid)
- `mountpoint-s3-fs/src/fs.rs`, `fs/config.rs`, `fs/flags.rs` (FOPEN_DIRECT_IO, CacheConfig, INIT caps)
- `mountpoint-s3-fs/src/prefetch.rs` (window sizes, seek tolerances, initial request size)
- `mountpoint-s3-fs/src/superblock.rs` (lookup = HeadObject + ListObjectsV2)
- `mountpoint-s3-fs/src/fuse.rs`, `fuse/config.rs` (mount options; getlk/setlk unsupported)
- `mountpoint-s3-fuser/src/mnt/fuse_pure.rs` (mount(2) first, fusermount fallback)

Search results consulted (pages themselves partly blocked):

- https://docs.aws.amazon.com/AmazonS3/latest/userguide/mountpoint-installation.html
- https://aws.amazon.com/about-aws/whats-new/2025/11/mountpoint-amazon-s3-amazon-linux-2023
- https://aws.amazon.com/blogs/aws/mountpoint-for-amazon-s3-generally-available-and-ready-for-production-workloads/
- https://medium.com/@maksym.lutskyi/a-comparative-analysis-of-mountpoint-for-s3-s3fs-and-goofys-9a097a25
- https://computingforgeeks.com/s3-files-vs-mountpoint-vs-s3fs/
- https://github.com/kahing/goofys
- https://github.com/awslabs/mountpoint-s3/issues/953 (slow throughput reading many small files)
- https://github.com/h5py/h5py/issues/2001 (HDF5 turns a slice into many small header reads)
- https://aws.amazon.com/s3/pricing/ (GET $0.0004/1000, LIST $0.005/1000)
- https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-network-bandwidth.html

Measured in this session (not AWS): `/dev/fuse` present, `CapEff` includes `CAP_SYS_ADMIN`,
`mount(2)` with type `fuse` returns 0; h5py 3.16 / HDF5 2.0.0 offers `sec2`/`stdio`/`core` but not
`direct`; xarray 2026.7.0 `H5netcdfBackendEntrypoint` forwards `driver_kwds` into `h5netcdf.File`
and on to `h5py.File`.
