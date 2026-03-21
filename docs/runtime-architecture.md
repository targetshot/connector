# Runtime Architecture

This document describes the current `ts-connect` runtime boundaries after the `P0` stabilization wave.

It is not meant to be a full design manifesto. Its purpose is practical:

- show which module owns which part of the runtime
- explain what still lives in `ui/app.py`
- document which future extractions are low-risk vs. not worth doing yet

## Top-Level Runtime Model

`ts-connect` consists of four main runtime surfaces:

1. `ui`
   - FastAPI admin UI
   - settings persistence
   - health summary
   - Club Plus / Keygen interaction
   - connector orchestration entrypoints

2. `update-agent`
   - isolated sidecar with Docker access
   - starts the update runner
   - exposes a small authenticated HTTP API

3. `update-runner`
   - performs git/image/compose update work
   - validates post-update stabilization
   - specifically handles MirrorMaker recovery after updates

4. `host-agent`
   - optional host-level helper outside Docker
   - OS package updates and orchestrated reboot

## Current Module Boundaries

### `ui/app.py`

`ui/app.py` remains the main application shell. It still owns:

- FastAPI route registration
- Jinja page rendering
- SQLite settings access and local state reads
- health summary assembly
- storage ownership preflight endpoints
- backup/export HTTP endpoints
- license/settings forms and request handling
- middleware, session auth, and top-level wiring

`app.py` is still large, but it is now primarily the integration shell rather than the place where every subsystem's internal logic lives.

### `ui/security_bootstrap.py`

Owns security/bootstrap concerns for the UI process:

- admin password bootstrap and hashing
- generated admin password handling
- session secret persistence and hardening
- private secret file permission correction
- thin auth guards used by the UI

This boundary is stable and should stay separate from route logic.

### `ui/operations_runtime.py`

Owns the operational runtime logic that the UI calls into:

- update-agent and host-agent HTTP interaction
- update-state reconciliation
- update launch orchestration
- auto-update sync support
- connector apply/retry orchestration
- MirrorMaker config/restart flow
- source replication apply/status helpers

This is the main extracted orchestration boundary and should absorb future connector/update workflow logic before `app.py` grows further.

### `ui/update_runner.py`

Owns the long-running update job:

- git refresh
- compose pull/build/up
- health-gated stabilization
- MirrorMaker restart/recovery after update
- update log streaming into state

It should remain free of HTTP concerns.

### `ui/update_agent.py`

Owns the small authenticated sidecar API for update execution:

- token-guarded HTTP endpoints
- single-job locking
- subprocess launch of `update_runner`
- minimal container/replication helper endpoints

The update-agent should stay intentionally small. It is a control-plane shim, not a second UI runtime.

### `ui/update_state.py`

Owns persistence for update progress/state:

- atomic JSON state file writes
- log buffering
- small merge/ensure helpers

This module exists to keep update-state file semantics out of `app.py` and `update_runner.py`.

### `ui/backup_runtime.py`

Owns small reusable backup scheduling/file helpers:

- backup directory resolution
- host display path resolution
- daily scheduling
- retention pruning
- backup file listing

It is intentionally tiny and utility-focused.

### `ui/live_verification.py`

Owns live operator verification for the Club Plus runtime:

- reading local runtime artifacts
- checking whether a connector is locally unlocked
- producing a structured verification report

This module is read-only by design.

### `host-agent/host_agent.py`

Owns optional host-level operations:

- package update checks
- package upgrade execution
- reboot orchestration
- host state persistence
- token-guarded HTTP API for the UI

This process should remain isolated from the containerized UI/update plane.

## Supporting Files

### `ui/templates/index.html`

This template is the operator control surface. It intentionally contains a lot of product/UI behavior, but not business logic that belongs in Python modules.

### Runbooks

Operational detail has been moved into focused docs:

- [operator-operations-runbook.md](./operator-operations-runbook.md)
- [keygen-activation-runbook.md](./keygen-activation-runbook.md)
- [topic-contract.md](./topic-contract.md)

The README should stay as the entrypoint and link to these docs, not repeat them.

## What Is Still Intentionally Centralized

The following still live in `ui/app.py` on purpose:

- route wiring
- request/response adaptation
- template context assembly
- local settings persistence
- the final health-summary aggregation

These are cross-cutting seams. Splitting them too aggressively would create extra indirection without reducing real risk.

## Next Safe Extraction Targets

If future feature work grows again, the next low-risk extraction targets are:

1. local settings/data-store helpers from `ui/app.py`
2. health-summary presentation/aggregation helpers
3. license persistence and local license-file sync helpers
4. backup/export orchestration above the already-extracted `backup_runtime`

These should only be extracted when one of these conditions is true:

- a file gains new feature logic and the boundary becomes obvious
- tests already isolate the behavior cleanly
- the new module would have a clear single responsibility

## What Not To Extract Prematurely

Avoid splitting just for line-count reduction when the result would scatter the request flow:

- route handlers that only adapt HTTP to one existing runtime call
- tiny helpers that already have a clear home
- health-card formatting that is still tightly coupled to the UI shell

The goal is fewer ambiguous ownership zones, not more files.

## Practical Rule For Future Changes

When adding new `ts-connect` functionality:

- put pure operational workflow into `operations_runtime.py`
- put startup/auth secret handling into `security_bootstrap.py`
- put long-running update execution into `update_runner.py`
- keep `app.py` as the route shell unless a new boundary is clearly reusable

That keeps future cleanup incremental instead of forcing another large refactor later.
