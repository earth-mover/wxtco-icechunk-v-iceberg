# 014 Controller credentials for the cloud session

**Status:** DECIDED 2026-09-18

The coding-agent cloud session that drove the study could not use AWS SSO or OIDC federation, so it
held one static access key for a dedicated IAM user with a narrow policy
(`scripts/aws/iam_policy_wxtco_controller.json`): launch, command, and terminate instances tagged
`project=wxtco`; describe calls; `iam:PassRole` for the job-instance role; and get/put under the
`_code/`, `_logs/`, `_progress/` control prefixes of the study bucket. It had no access to data
prefixes, Secrets Manager, or IAM. Data movement and the Arraylake token stayed on the EC2 instance
role. The user and key are deleted at project end (`scripts/aws/README.md`, Teardown).
