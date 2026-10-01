# Infrastructure access checklist

Run at the start of any session that touches cloud resources. Update the status column.
Commands are safe (read-only).

| resource | check | status 2026-09-18 |
|---|---|---|
| AWS SSO session | `aws sts get-caller-identity --profile PowerUserAccess-<ACCOUNT_ID>` | ✅ 2026-09-18. IAM reads/writes need `AdministratorAccess-<ACCOUNT_ID>` |
| Source bucket (anon) | `aws s3 ls s3://met-office-global-ensemble-model-data/global-ensemble/ --region eu-west-2 --no-sign-request` | ✅ works |
| Study S3 bucket | `aws s3 ls s3://em-tco-mogreps/` | ✅ created 2026-09-18, tagged project=wxtco, public access blocked, request metrics on |
| Arraylake CLI | `uvx --from arraylake arraylake auth status` | ⬜ headless: use `ARRAYLAKE_TOKEN` from `.env` |
| Arraylake org with Iceberg | `al org list`, check `feature_flags` has `iceberg` | ✅ org `wxtco`, `iceberg` flag set (Ryan, 2026-09-18) |
| Virtual chunk access policy on `wxtco-netcdf` | web app → org `wxtco` settings → VCAPs | ⚠️ API call 404s with the `ema_` token (needs org admin). Ryan to add in web app: bucket `wxtco-netcdf`, subprefix empty, private |
| Arraylake API token for headless runs | `ARRAYLAKE_TOKEN` in `.env`; Secrets Manager `wxtco/arraylake-token` | ✅ both, 2026-09-18 |
| EC2 quota, us-east-1 | `aws service-quotas get-service-quota --service-code ec2 --quota-code L-1216C47A` | ⬜ check before first c7i.16xlarge launch |
| Athena | `aws athena list-work-groups` | ⬜ stretch goal only |
| GitHub | `gh auth status` | ✅ rabernat, scopes repo/workflow |
| uv / Python | `uv --version` | ✅ uv 0.11.25 |
| Modal / Coiled | `which modal coiled` | ❌ not installed; not needed if decision 007 holds |

## Created 2026-09-18 (scripts/aws/README.md steps 1-5)

| resource | id |
|---|---|
| bucket | `s3://em-tco-mogreps` (us-east-1) |
| EC2 instance role / profile | `arn:aws:iam::<ACCOUNT_ID>:role/wxtco-ec2`, instance profile `wxtco-ec2`, policies `wxtco-ec2` + `AmazonSSMManagedInstanceCore` |
| secret | `wxtco/arraylake-token` (Secrets Manager, us-east-1) |
| Arraylake delegation role | `arn:aws:iam::<ACCOUNT_ID>:role/wxtco-arraylake`, external ID in Secrets Manager `wxtco/arraylake-external-id` |
| Arraylake bucket configs (org `wxtco`) | `wxtco-storage` → `s3://em-tco-mogreps/arraylake` (default), `wxtco-netcdf` → `s3://em-tco-mogreps/netcdf` |
| controller user | `wxtco-controller`, policy `wxtco-controller`, one active access key; secret stored at `~/.wxtco-controller-key.json` on Ryan's laptop only |

Controller key tested: `sts`, `ec2:Describe*`, `ssm:GetParameter`, put/list under `_code/` succeed;
`s3:ListBucket` on `netcdf/` and `secretsmanager:GetSecretValue` are denied, as designed.

## Cloud session diagnostics (claude.ai/code environment `wxtco`, 2026-09-18, session 002)

Run per `docs/infra/cloud-session.md` "First-session diagnostics". Egress denials carry
`x-deny-reason: host_not_allowed` and the body "Host not in allowlist: <host>".

| check | result |
|---|---|
| `uv --version` | ✅ uv 0.8.17 (image) |
| `command -v aws` | ❌ no aws cli; `uv tool install awscli` cannot run (PyPI blocked) |
| `uv sync --frozen` | ❌ 403 `host_not_allowed` for `files.pythonhosted.org`; `curl https://pypi.org/simple/icechunk/` also 403 |
| `uv run pytest -q` | ❌ not run (no venv) |
| boto3 `sts.get_caller_identity` | ❌ boto3 not installed; `curl https://sts.amazonaws.com/` fails: CONNECT 403 from proxy |
| `sts.us-east-1`, `s3.amazonaws.com`, `em-tco-mogreps.s3.amazonaws.com`, `ec2.us-east-1`, `ssm.us-east-1` | ❌ all CONNECT 403 (egress policy) |
| `curl https://api.earthmover.io/` | ✅ 200 |
| `AWS_ACCESS_KEY_ID`, `AWS_DEFAULT_REGION`, `ARRAYLAKE_ORG` | ✅ present in the environment |
| system `python3` | 3.11, no boto3 |

Conclusion: the `wxtco` environment allowlist must add `pypi.org`, `files.pythonhosted.org`, and
`*.amazonaws.com` (at minimum `sts`, `ec2`, `ssm`, `s3` in `us-east-1`) before any plan can run.
The proxy treats `pypi.org` / `files.pythonhosted.org` as no-proxy hosts, so the 403 comes from the
environment egress policy, not from the agent proxy.

## Cloud session diagnostics (environment `wxtco`, 2026-09-18, session 003, after allowlist fix)

| check | result |
|---|---|
| `uv --version` | ✅ uv 0.8.17 |
| `command -v aws` | ✅ `/root/.local/bin/aws`, aws-cli 1.46.1 (setup script now succeeds) |
| `uv sync --frozen && uv run pytest -q` | ✅ 68 packages audited, 1 passed |
| `curl https://pypi.org/simple/boto3/` | ✅ 200 |
| `uv run --with boto3 python -c "...sts.get_caller_identity()"` | ✅ `arn:aws:iam::<ACCOUNT_ID>:user/wxtco-controller` (boto3 is not a project dependency; ephemeral install proves PyPI download) |
| `aws sts get-caller-identity` | ✅ same ARN |
| `curl https://s3.us-east-1.amazonaws.com/` | ✅ 307 (reachable) |
| `curl https://api.earthmover.io/` | ✅ 200 |
| `AWS_ACCESS_KEY_ID`, `AWS_DEFAULT_REGION`, `ARRAYLAKE_ORG` | ✅ present |

Conclusion: allowlist fix works. Plan 01 execution may start.

## Local facts

  (clone lives outside this repo; re-clone as needed).
- Sibling repos in `~/gh/earth-mover/`: `arraylake`, `icechunk`, `zax`, `zax-prototype`,
  `snowflake-app`, `zax-gcm-examples`. Useful for API reference.
- Helper: `scripts/check_access.sh` runs the table above.
