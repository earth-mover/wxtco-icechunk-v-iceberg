# EC2 jobs

Run from a cloud session or a laptop with the `wxtco-controller` key. Commit before launching:
the instance runs the committed tree, uploaded as `_code/<sha>.tar.gz`.

    export AWS_PROFILE=PowerUserAccess-<ACCOUNT_ID> AWS_DEFAULT_REGION=us-east-1 ARRAYLAKE_ORG=wxtco
    ID=$(scripts/ec2/launch.sh c7i.16xlarge wxtco-ingest)
    # wait for /opt/wxtco/READY (about 3 min): aws ssm send-command ... 'cat /opt/wxtco/READY'
    # /opt/wxtco/FAILED instead of READY means cloud-init stopped; read /var/log/cloud-init-output.log
    scripts/ec2/run_job.sh "$ID" table-cycle1 -- uv run wxtco ingest table --cycle 2026/09/16/T0000Z --workers 48
    scripts/ec2/terminate.sh "$ID"   # prints hours; record in docs/findings/etl/<method>.csv

Ingest instance: c7i.16xlarge. Benchmark instance: m7i.4xlarge. Always terminate; a forgotten
c7i.16xlarge costs ~$70/day.

Use `uv run wxtco jobs status` to read `_progress/`, and `uv run wxtco jobs log <name>` to tail a
finished job log. `run_job.sh` uploads the log when the job ends, so an absent log means the job
still runs (or the instance died).

## Scripts

| script | what it does |
|---|---|
| `launch.sh <type> <name>` | tarball of `HEAD` to `_code/<sha>.tar.gz`, then one tagged instance. Prints the instance id on stdout |
| `user_data.sh` | cloud-init on the instance: uv, the tarball, `uv sync --frozen`, `.env` with the Arraylake token. Writes `/opt/wxtco/READY`, or `/opt/wxtco/FAILED` on any error |
| `run_job.sh <id> <name> -- <cmd...>` | SSM `AWS-RunShellScript`, 48 h timeout. The job logs to `/opt/wxtco/<name>.log` and the log goes to `_logs/<name>.log` at the end |
| `terminate.sh <id>` | terminates and prints the instance lifetime in hours |

## Assumptions

- **Default VPC.** `launch.sh` sends no subnet and no security group, so EC2 uses the default VPC
  of us-east-1 and its default security group. That subnet must give outbound internet (a public
  subnet with auto-assign public IP, or a NAT), because the instance reaches S3, PyPI, `astral.sh`
  and the SSM endpoints. Set `WXTCO_SUBNET_ID` and `WXTCO_SECURITY_GROUP_ID` to override; the
  controller policy already allows both resource types on `RunInstances`.
- **SSM Agent and the instance role.** AL2023 has the agent preinstalled. `wxtco-ec2` must carry
  `AmazonSSMManagedInstanceCore`, or `run_job.sh` finds no instance to send to.
- **The region is us-east-1.** Every script uses `AWS_DEFAULT_REGION` and falls back to us-east-1,
  because the controller policy is us-east-1 only.
- **No slashes in `WXTCO_ORG`.** `launch.sh` substitutes it into `user_data.sh` with `sed` and `/`
  as the delimiter.
- **The command must not need a login shell.** `run_job.sh` joins the command with `shlex.join`
  and sources `.env`; quotes are safe, but shell built-ins of your local shell are not available.

## Controller permissions

`scripts/aws/iam_policy_wxtco_controller.json` is the whole budget these scripts have:

- `ec2:RunInstances` only when the request carries the tag `project=wxtco`. `launch.sh` always
  sends `--tag-specifications ResourceType=instance,Tags=[{Key=project,Value=wxtco},...]`. Remove
  that tag and the launch is denied.
- `ec2:TerminateInstances` and `ec2:StopInstances` only on instances that already carry
  `project=wxtco`. An instance launched by hand without the tag cannot be terminated by this key.
- `iam:PassRole` for `wxtco-ec2` only. `--iam-instance-profile Name=wxtco-ec2` must stay as is.
- `ssm:SendCommand` only for `AWS-RunShellScript` and only on instances tagged `project=wxtco`.
- `ssm:GetParameter` for the AL2023 AMI lookup; `ec2:Describe*` for `terminate.sh`.
- S3 `GetObject`/`PutObject` under `_code/`, `_logs/` and `_progress/` only, plus `ListBucket`
  limited to those three prefixes. The controller cannot read or write the data itself; the
  instance does that under the `wxtco-ec2` role.
- Not granted: `secretsmanager:GetSecretValue`. The Arraylake token is fetched on the instance by
  `user_data.sh` under the instance role, never on the controller.

## Notes on the scripts

- The AWS CLI base64-encodes `--user-data` itself, so `launch.sh` passes the rendered script as
  `file://`. Passing a base64 string here encodes it twice and cloud-init runs nothing.
- AL2023 ships aws-cli v2. `user_data.sh` installs only `tar` and `gzip`; installing the `awscli`
  package would add a v1 that shadows it.
- `user_data.sh` disables `xtrace` around the secret, so the token stays out of
  `/var/log/cloud-init-output.log`.
- The `.env` on the instance sets `WXTCO_ORG`, which `wxtco.config.Settings` reads first
  (`ARRAYLAKE_ORG` is the fallback).
- SSM gives a bare environment, so `run_job.sh` sets `HOME=/root` and `UV_CACHE_DIR`, and runs
  the setup under `set -e`: a missing `.env` aborts the job instead of running it without
  credentials. `set +e` comes back before the job, so the log upload always happens.
- `run_job.sh` uses no `nohup`. SSM kills the process group when the document returns, so the
  job must stay in the foreground of the SSM command.
- `.env` values are single-quoted. An unquoted token that holds `#` loses its tail.
- `launch.sh` tags the root volume as well as the instance, so Cost Explorer sees the EBS cost.
- SSM `executionTimeout` is capped at 172800 s (48 h). A longer job must be split by cycle.
