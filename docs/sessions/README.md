# Session protocol

Each Claude Code session is independent. State lives in the repo, not in chat.

## Start of session

1. Read `CLAUDE.md`, `STATUS.md`, `docs/decisions/README.md`.
2. Read the newest file in this directory.
3. Check `docs/infra/access-checklist.md` if the session touches cloud resources.

## During the session

- Record any non-obvious fact about data, infra, or tooling in `docs/findings/notes.md`
  as soon as it is learned. Do not rely on memory.
- New decision needed → add a file in `docs/decisions/` and mark it in the register.
- Measurements go in `docs/findings/` as CSV or Markdown tables with the exact command
  that produced them.

## End of session

1. Write `docs/sessions/YYYY-MM-DD-NNN.md`: what was done, what was learned, what is next,
   what is blocked. Under one page.
2. Update the "Now" block and the log in `STATUS.md`.
3. Commit. Message: `session NNN: <one line>`.

## Subagents

Coordinator sessions use Fable. Subagents use Opus. Every dispatch prompt begins with the
verbatim block in `CLAUDE.md`. Use `docs/sessions/SUBAGENT_TEMPLATE.md`.
Subagents write results to a file path given in the prompt; the coordinator merges.
