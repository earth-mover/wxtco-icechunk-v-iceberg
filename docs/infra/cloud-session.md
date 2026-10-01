# Running this project from a Claude Code cloud session

The cloud VM (4 vCPU, 16 GB) is a controller. It edits code, runs fixture tests, launches EC2
jobs, and reads job status. All data movement happens on EC2 under the `wxtco-ec2` instance role.

## Environment configuration (claude.ai/code, once)

| setting | value |
|---|---|
| repository | `earth-mover/wxtco-icechunk-v-iceberg` (Claude GitHub App installed) |
| network access | Custom: default allowlist plus `api.earthmover.io`, `pypi.org`, `files.pythonhosted.org`, `*.amazonaws.com`. Session 002 (2026-09-18) saw PyPI and AWS denied with `x-deny-reason: host_not_allowed`; the allowlist must carry them explicitly. Session 003 confirmed the fix: PyPI, STS, S3 all reachable. apt mirrors, `astral.sh`, and `docker.com` return 403; `uv` is preinstalled in the image; the setup script is best-effort and never fails |
| setup script | contents of `scripts/cloud_setup.sh` |
| environment variables | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` (user `wxtco-controller`), `AWS_DEFAULT_REGION=us-east-1`, `ARRAYLAKE_ORG=wxtco` |
| API credential (optional) | `ARRAYLAKE_TOKEN` as Bearer for host `api.earthmover.io`, only if the session itself must call Arraylake |

The IAM user holds only the control-plane policy in decision 014. Never put the `wxtco-ec2` role, a
copy-bucket write key, or the Arraylake token in environment variables. Environment variables are plain text to anyone who can use the environment.

## First-session diagnostics

Run these before anything else and record the results in `docs/infra/access-checklist.md`:

    uv --version; command -v aws || echo "no aws cli"
    uv sync --frozen && uv run pytest -q          # proves PyPI is reachable for uv
    uv run python -c "import boto3; print(boto3.client('sts').get_caller_identity()['Arn'])"
    curl -sS -o /dev/null -w '%{http_code}\n' https://api.earthmover.io/

If `uv sync` cannot reach PyPI, the environment's network allowlist is wrong; stop and report.
If `aws` is missing, the EC2 scripts in Plan 04 must use boto3 instead of the CLI; add that as a
step to Plan 04 Task 8 (`scripts/ec2/*.py` via `uv run`).

## What the session does

    uv run pytest -q                                  # fixture tests, local
    scripts/ec2/launch.sh c7i.16xlarge wxtco-ingest   # start a job machine (Plan 04 Task 8)
    scripts/ec2/run_job.sh <id> table-c01 -- uv run wxtco ingest table --cycle ...
    uv run wxtco jobs status                          # read _progress/ and _logs/
    scripts/ec2/terminate.sh <id>

Jobs are detached (SSM + nohup). A session that dies from inactivity loses nothing: the job
continues on EC2, and every ingest is idempotent per unit, so a new session resumes by rerunning
the same command. Use `/loop 20m uv run wxtco jobs status` to keep a session polling.

## Code delivery to EC2

EC2 never talks to GitHub. `scripts/ec2/launch.sh` uploads `git archive HEAD` to
`s3://em-tco-mogreps/_code/<sha>.tar.gz` and the instance unpacks it. Commit before launching.

## Rotation

Delete the `wxtco-controller` access key when the study ends (`scripts/aws/README.md`, Teardown).
