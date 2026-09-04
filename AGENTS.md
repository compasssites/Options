# Agent Instructions

<!-- compass:start:global -->
# Global Defaults

- Prefer simple, maintainable solutions. Avoid overengineering.
- Prefer the Cloudflare-native stack unless the project says otherwise. Do not silently change stack.
- Make small, understandable changes. Prefer targeted edits over broad rewrites unless asked.
- Do not invent credentials, services, or architecture.
- Be concise, pragmatic, and low-repetition. Explain assumptions briefly.
- Inspect the repo before asking broad setup questions.
- `AGENTS.md` and `CLAUDE.md` are already in your context; never read them again. Read `.compass/ui.md` for UI work, and `PROJECT-NOTES.md` or deploy docs only when the task touches deployment.
- After meaningful code changes, run checks and ship with `compass-ship "message"`. Do not stop at an uncommitted handoff.

## Efficient execution

- Optimize for the shortest safe path to the requested outcome. Do not trade away correctness, security, or required verification.
- For a narrow request, inspect only the relevant files, symbols, and nearby context. Prefer `rg` and narrow line ranges; do not dump large files or repository trees without a concrete need.
- Reuse context already established in the current thread. Do not reread unchanged instructions or files already in context, except where a skill requires a fresh read.
- No tree dumps: no `find .`, `ls -R`, or `tree` below depth 2. Never list or read `node_modules`, `dist`, `out`, `build`, `.wrangler`, coverage, lockfiles, minified or generated files, or packaged binaries.
- Cap command output. Pipe installs, builds, and tests through `tail -30`. Never run watch modes, dev servers, or log tailers in the foreground; anything that can hang gets a timeout.
- No ritual `git status`, `git log`, or `git diff` at session start. Run them only when the task concerns existing changes; `compass-ship` shows the file list before committing.
- For screenshot-led UI work, fix the visible requested issues first. Do not add adjacent features, redesign other areas, or expand scope unless required for the fix.
- Start with the smallest targeted patch. If it fails, inspect the exact surrounding context and retry narrowly instead of broadening the rewrite.
- Never run a repository-wide formatter for a targeted change or against a pre-existing unformatted file. Format only touched code, then check the diff size.
- Batch independent reads and checks into one tool call when practical.
- Avoid duplicate verification. Know what `compass-ship` already runs; during implementation use only the fastest targeted check, then let the wrapper run the full suite once.
- Decide reversible choices yourself and state the assumption in one line. Ask at most one clarifying question, and only when the readings lead to materially different work.
- Final message under 150 words for ordinary tasks: what changed, what was verified, what is left. Do not restate the diff. Write a `plans/` file only for work spanning sessions.
- Stop when the requested result is implemented, checks pass, and it is shipped. Do not add optional audits, deployment polling, or status checks.

## Finding the code — search, don't survey

Broad exploration is an escalation, not the opening move.

- Start from the literals in the request: the visible UI string, route, field, table, endpoint or error message. For "change Amount Payable to Net Payout", the first action is `rg -n "Amount Payable" src`, not reading the payout module.
- Budget for an ordinary fix: about three targeted searches and six files opened. Crossing it is fine when the task genuinely spans modules — say so in one line and carry on.
- Once you can name the input, the processing and the output for the change, stop looking and implement. Read analogous code only when an existing convention is genuinely unclear.
- Read the Feature Map in the Project section before searching. If it is missing or wrong for an area you just worked in, fix it in the same turn.
- Scratch files, exports, dumps, SQL experiments and debugging artifacts go in `.agent-tmp/` and nowhere else. Delete the ones you created when the task ends.
- Delete only files you created in this task. Never delete a pre-existing untracked file because it looks unused; mention it instead.

## Load a skill instead of carrying the knowledge

Long knowledge lives in skills, not in this file. Load the skill when the task calls for it:

- `compass-ui` — any interface work, before touching a component.
- `compass-workflow` — deploys, migrations, "is it live", and the plain-text summary to send a client after a fix.
- `compass-mcp` — anything involving MCP servers or app connectors.
- `simple-app-auth` — email allowlist, one-time setup codes, email-plus-password login.

Anything over roughly 2 KB that applies to fewer than half the repos belongs in a skill, not in an always-loaded memory file. If you find yourself adding a long section here, add a skill instead.

## Finish the whole job — never leave manual steps for the user

The owner is not a technical user and must never be handed terminal commands, migrations, or deploy steps to run. Whatever a change needs to reach production, the agent runs it, verifies it, and only then reports done.

- Run database migrations yourself immediately after adding migration files. A git-push deploy never applies them.
- In a git-connected auto-deploy repo, a successful push completes the deployment. Do not poll deployment lists, CI, or Cloudflare status to prove it landed.
- Call live URLs only when the change altered URL or API behaviour and an HTTP test gives real evidence, or when the owner asks.
- When the Project section says "Unknown" for something you just worked out (deploy style, build command, database, worker name), write the real value into both the repo's `AGENTS.md` and `~/CompassAgentMemory/projects/<repo-name>.md` in the same turn.
- Details, including deploy styles and what counts as verification, are in the `compass-workflow` skill.

## Client-reported changes

When the owner relays client feedback, ship the fix and then, without being asked, output a plain-text summary they can paste into an email: no markdown, one sentence per item, no technical terms, in its own clearly separated block. The full rules are in `compass-workflow`, reference `client-email.md`.

## Shipping — one command, one approval

The only approved way to commit is:

```
/Users/macpro/CompassAgentMemory/bin/compass-ship "commit message" [file ...]
```

It stages, refuses to proceed if a staged file looks like a secret, runs the project check, commits, and pushes as a single tool call, so it costs one approval instead of three. With no file list it stages everything.

In a Codex session the workspace sandbox refuses writes to `.git`, so the first attempt fails on `index.lock` and the wrapper has to be re-run escalated. Ask for the elevated run with the first call rather than letting it fail and retrying — it is one approval either way, and the failed attempt is pure waste. This is an upstream Codex limitation, not a Compass one, and it applies even in a trusted project.

Do not run `git add`, `git commit`, or `git push` separately. They still work and still prompt; they are just three interruptions instead of one. Force-push and `reset --hard` are denied outright.

## No browser or visual testing

Never launch a browser, headless browser, screenshot tool, or visual-regression run. These are denied at the policy layer; the attempt will fail and cost you a turn.

The owner supplies screenshots when a visual check is needed. Verify with typecheck, build, lint, unit tests, and `curl` against the preview or live URL. If you genuinely believe a visual check is required, say so and stop.

Close what you start. A `vite` or `wrangler dev` spawned to verify something must be stopped in the same turn.

## Subagents

Do not spawn subagents unless the user asks for one in that message. They re-derive context from cold and cost many times a normal turn. Handle multi-part work inline.

## Escalation is normal — do not let it stop you

These cost one approval each, and you should go ahead and ask: applying a D1 migration, any non-SELECT `wrangler d1 execute`, `wrangler deploy` or `pages deploy`, `wrangler secret`, R2 object writes, KV key writes.

This does not weaken "Finish the whole job". Ask, get the approval, run it, verify it, then report done. What you must not do is skip the step or hand it to the owner.

DNS and zone settings are the exception: denied with no escalation path. If a DNS change is needed, say so and let the owner make it in the Cloudflare dashboard.

## Where these rules are enforced

Prose is advice, and a model under pressure can talk itself past advice. The rules above that matter are also compiled into `permissions.deny` / `permissions.ask` and a `PreToolUse` hook, from `~/CompassAgentMemory/memory/60-policy.json`. Where this file says something is denied, it is denied by the harness, not by good intentions.
<!-- compass:end:global -->

<!-- compass:start:security -->
# Security Rules

- Never read, print, export, reveal, or exfiltrate secrets.
- Never ask the user to paste Cloudflare API tokens or secrets into chat.
- Never commit `.env`, `.dev.vars`, private keys, API tokens, database passwords, wallet files, or Wrangler auth files.
- Never run commands that intentionally dump environment variables or secrets.
- Prefer existing local authenticated sessions.
- If a secret is needed, instruct the user to set it manually using the project's documented safe command.
- Before commit, check for accidental secrets.
- If unsure whether a file contains secrets, do not display it fully.
- Do not expose wallet files, DB connection secrets, Cloudflare credentials, or Wrangler auth files.
<!-- compass:end:security -->

<!-- compass:start:stack -->
# Directus / Postgres / CapRover Defaults

- Avoid touching persistent volumes without explicit instruction.
- Avoid destructive database migrations.
- Prefer backups before schema changes.
- Be careful with Docker image/custom image changes.
- For CapRover, inspect existing app/service names before suggesting commands.
- Never expose DB passwords or connection strings.
<!-- compass:end:stack -->

<!-- compass:start:project -->
# Project: Options

## Basic Info

Path: /Users/macpro/Dev/Apps/Options
Detected stack: directus-pg-caprover
Main app type: Unknown
Deploy style: Unknown
Git branch: main
Deploy trigger: Unknown (options: push-to-main auto-deploys via Cloudflare git integration / manual wrangler deploy / other)

## Agent Rules

- Read AGENTS.md first.
- Follow global and security rules.
- Use project-specific commands below.

## Project Commands

Build: Unknown
Test: Unknown
Check: Unknown
Deploy: Unknown
Commit/push policy: commit and push meaningful changes to the current branch after checks pass unless the user explicitly says not to

## Interface Profile

UI personality: Unknown (choose institutional / product / expressive)
Component foundation: Unknown
Form pattern: short create/edit flows in accessible modals; login, search, filters and long workflows stay dedicated
Motion profile: restrained, purposeful, 150–220ms, reduced-motion supported; no preview variants

## Cloudflare Notes

Wrangler config: None found
D1 bindings: Unknown
R2 bindings: Unknown
Pages project: Unknown
Worker name: Unknown

## Database Notes

Database type: Unknown
Migration style: Unknown
Migrations: Unknown (options: manual — agent must run them / automatic / none)
Backup notes: Unknown

## Manual Notes

Add human notes here.
<!-- compass:end:project -->

<!-- compass:start:workflows -->
# Workflows

## Standard Agent Workflow

1. `AGENTS.md` and `CLAUDE.md` are already in your context. Do not read them again. Read `.compass/ui.md` only for UI work and `PROJECT-NOTES.md` or deploy docs only when the task touches deployment.
2. Inspect only the files the request names and their direct neighbours.
3. Make the smallest correct change.
4. Run one targeted check at the end (typecheck or build), output tailed.
5. Ship with `compass-ship "message"`; it stages, scans for secrets, runs the project check, commits, and pushes in one approval. Never stage, commit, or push by hand.
6. Report in under 150 words: what changed, what was verified, what is left.

## Standard Safety Workflow

- Never print, read whole, or commit `.env`, `.dev.vars`, tokens, keys, or wallet files. `compass-ship` refuses secret-like files; do not work around it.
<!-- compass:end:workflows -->
