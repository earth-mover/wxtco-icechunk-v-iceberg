# Weather Forecast Data TCO Analysis - Coding Agent Guidance

This is a big complex project, whose goals are described in `./project_plan.md`

At a high level, we want to proceed in the following way:

1. Create the proper structure and harness to allow us to progress via independent sessions, with learnings and progress recorded within the repo itself. We also need to plan for parallel dispatch of subagents to speed up development.
2. Clarify open questions and make decisions on infrastructure needed for the project.
3. Make a detailed implementation plan that can be executed robustly with minimal human supervision.
4. Verify access to all necessary infrastructure (remote data storage, remote execution / orchestration environments, secrets and credentials, etc.) needed for the plan and document usage for subsequent sessions.
5. Implement
6. Document and synthesize findings.

## Subagent Dispatch

We use Fable primarily for the master planning and coordination.

Subagents should also use Opus, not Fable.

Every Agent dispatch prompt opens with this block, verbatim:

> WRITE ALL COMMENTS AND DOC COMMENTS IN ASD-STE100 SIMPLIFIED TECHNICAL ENGLISH.
> VERY BRIEF, 1-2 LINE COMMENTS TARGETED AT AN EXPERT READER.
## Where state lives (read in this order at session start)

1. `STATUS.md` — current phase, what is blocked, what to do next, session log.
2. `docs/decisions/README.md` — decision register; `NEEDS RYAN` items block progress.
3. `docs/sessions/README.md` — session protocol; newest `docs/sessions/*.md` for last context.
4. `docs/infra/access-checklist.md` — cloud access state; `scripts/check_access.sh` runs it.
5. `docs/findings/notes.md` — facts learned about data, infra, tooling. Append, cite commands.

Subagent dispatch template: `docs/sessions/SUBAGENT_TEMPLATE.md`. Python via `uv`; package `wxtco`.
