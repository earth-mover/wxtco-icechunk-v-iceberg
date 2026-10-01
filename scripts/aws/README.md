# AWS bootstrap

Replace `<ACCOUNT_ID>` below and in the `iam_policy_*.json` files with your AWS account ID
(`sed -i 's/<ACCOUNT_ID>/123456789012/g' scripts/aws/*.json`). The study ran in a sandbox account, region `us-east-1`. Steps 1 and 3 work with
`PowerUserAccess-<ACCOUNT_ID>`; steps 2 and 4 are IAM writes and need
`AdministratorAccess-<ACCOUNT_ID>` (PowerUser denies all `iam:*` writes, verified 2026-09-18).
Run once, from a laptop with an active SSO session. Every command is idempotent or fails
harmlessly on rerun ("already exists").

    aws sso login --sso-session ryans-laptop-session
    export AWS_PROFILE=AdministratorAccess-<ACCOUNT_ID> AWS_DEFAULT_REGION=us-east-1
    cd <repo root>

## 1. Copy bucket

    scripts/aws/create_bucket.sh

## 2. Instance role for EC2 job machines

    aws iam create-policy --policy-name wxtco-ec2 --policy-document file://scripts/aws/iam_policy_wxtco.json --tags Key=project,Value=wxtco Key=BillingCategory,Value=Marketing
    aws iam create-role --role-name wxtco-ec2 --tags Key=project,Value=wxtco Key=BillingCategory,Value=Marketing \
      --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
    aws iam attach-role-policy --role-name wxtco-ec2 --policy-arn arn:aws:iam::<ACCOUNT_ID>:policy/wxtco-ec2
    aws iam attach-role-policy --role-name wxtco-ec2 --policy-arn arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore
    aws iam create-instance-profile --instance-profile-name wxtco-ec2
    aws iam add-role-to-instance-profile --instance-profile-name wxtco-ec2 --role-name wxtco-ec2

## 3. Arraylake token for the job machines

Create the standalone org and an `ema_` token first (app.earthmover.io). Then:

    aws secretsmanager create-secret --name wxtco/arraylake-token --secret-string "$ARRAYLAKE_TOKEN" --tags Key=project,Value=wxtco Key=BillingCategory,Value=Marketing

## 4. Controller user for the cloud session

The only credential the Claude Code cloud session holds. It can launch, command, and stop
`project=wxtco` instances and read the `_code/`, `_logs/`, `_progress/` prefixes. Nothing else.

    aws iam create-user --user-name wxtco-controller --tags Key=project,Value=wxtco Key=BillingCategory,Value=Marketing
    aws iam create-policy --policy-name wxtco-controller --policy-document file://scripts/aws/iam_policy_wxtco_controller.json --tags Key=project,Value=wxtco Key=BillingCategory,Value=Marketing
    aws iam attach-user-policy --user-name wxtco-controller --policy-arn arn:aws:iam::<ACCOUNT_ID>:policy/wxtco-controller
    umask 077; aws iam create-access-key --user-name wxtco-controller --output json > ~/.wxtco-controller-key.json

Copy `AccessKeyId` and `SecretAccessKey` from that file into the cloud environment variables as
`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` (see `docs/infra/cloud-session.md`). Do not
store them anywhere else.

## 5. Arraylake storage: delegation role, bucket configs, virtual chunk access policy

Arraylake needs its own way into the bucket. It assumes an IAM role in our account
(trusted principal `<EARTHMOVER_SIGNER_ACCOUNT_ID>`, Earthmover's signer service, with an external ID) and vends
short-lived credentials to clients. One role covers both uses:

| prefix | bucket config | purpose |
|---|---|---|
| `s3://em-tco-mogreps/arraylake/` | `wxtco-storage` (org default) | Icechunk repo metadata and chunks; Iceberg warehouse |
| `s3://em-tco-mogreps/netcdf/` | `wxtco-netcdf` | virtual chunk source; read-only; VCAP `public=False` |

    scripts/aws/setup_arraylake_storage.sh    # Administrator profile; idempotent

The script stores the external ID in Secrets Manager `wxtco/arraylake-external-id`. The VCAP
call needs org-admin rights on `wxtco`; if it prints `VCAP not set`, add it in the web app
(org settings → virtual chunk access policies → bucket `wxtco-netcdf`, subprefix empty).

## 6. Verify

IAM reads (`get-role`, `list-access-keys`) also need the Administrator profile.

    aws s3 ls s3://em-tco-mogreps/
    aws iam get-role --role-name wxtco-ec2 --query Role.Arn
    aws secretsmanager describe-secret --secret-id wxtco/arraylake-token --query Name
    aws iam list-access-keys --user-name wxtco-controller --query 'AccessKeyMetadata[].AccessKeyId'
    uv run python -c "from arraylake import Client; print([(b.nickname,b.is_default) for b in Client().list_bucket_configs('wxtco')])"

Record results in `docs/infra/access-checklist.md`.

## Teardown (project end)

    aws iam delete-access-key --user-name wxtco-controller --access-key-id <id>
    aws iam detach-user-policy --user-name wxtco-controller --policy-arn arn:aws:iam::<ACCOUNT_ID>:policy/wxtco-controller
    aws iam delete-user --user-name wxtco-controller
    aws secretsmanager delete-secret --secret-id wxtco/arraylake-token --force-delete-without-recovery
    # bucket: decide whether to keep the static copy; it is the "with originals" storage line.

## 7. Billing tags

Every project resource carries `project=wxtco` and `BillingCategory=Marketing` (Ryan, 2026-09-22). New
instances and volumes get both from `scripts/ec2/launch.sh`; the creation commands above tag IAM, the secret
and the bucket. To (re)apply the tags to everything that already exists, run with the PowerUser profile
(the `wxtco-controller` user cannot tag S3, IAM or Secrets Manager):

    scripts/aws/tag_billing.sh
