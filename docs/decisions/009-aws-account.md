# 009 AWS account

**Status:** DECIDED 2026-09-18. Earthmover Sandbox account `<ACCOUNT_ID>`.

`~/.aws/config` has SSO profiles for accounts `<ACCOUNT_ID>` (Admin + PowerUser) and
`<EARTHMOVER_SIGNER_ACCOUNT_ID>` (PowerUser), region us-east-1. No active session on 2026-09-18.

Recommendation: `PowerUserAccess-<ACCOUNT_ID>` unless that is production. Confirm which
account may host a ~7 TB bucket and multi-day EC2 runs, and any cost tagging convention.
