# Subagent dispatch template

Copy, fill the angle brackets, dispatch with `model: opus`.

```
WRITE ALL COMMENTS AND DOC COMMENTS IN ASD-STE100 SIMPLIFIED TECHNICAL ENGLISH.
VERY BRIEF, 1-2 LINE COMMENTS TARGETED AT AN EXPERT READER.

## Context
Repo: the repository root
Read first: CLAUDE.md, STATUS.md, docs/decisions/README.md, <task-specific files>.
Do not read or modify anything outside: <allowed paths>.

## Task
<one paragraph, one goal>

## Constraints
- Python via `uv`. Add dependencies to `pyproject.toml`, never ad hoc installs.
- No cloud writes unless the task says so. Read-only probes are fine.
- Do not touch STATUS.md or docs/sessions/. The coordinator does that.

## Deliverable
Write <file path>. Report in your final message: what you did, what you measured
(with the command), what failed, what you did not do.
```

## Parallel dispatch rules

- Tasks must not share output files.
- Tasks must not write to the same Arraylake repo or S3 prefix.
- Give each task a distinct scratch prefix: `s3://<bucket>/scratch/<task-id>/`.
