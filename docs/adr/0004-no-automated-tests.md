# ADR 0004: No automated tests

- Status: Accepted (risk accepted, no change)
- Date: 2026-09-27

## Context

The repo had 3 unit tests, for the version-vector order and the conflict rules
in `src/index.rs`. They have been deleted. There are no other tests, and no CI:
GitHub runs nothing on a push.

During development, changes were checked by hand:
- a three-node test on one machine, using a separate `HOME` and port for each
  node;
- runs between two real Macs.

Those scripts were never part of the repo and have been deleted too.

## Decision

Accept this risk. rust-sync has no automated tests and no CI.

## Consequences

- Nothing catches a change that breaks syncing, conflict handling or deletion
  before it reaches users. A bug here can overwrite or delete files on every
  node, and (see [ADR 0002](0002-no-copy-of-deleted-or-replaced-files.md)) no
  copy of the old version is kept.
- Every change must be checked by hand, on at least two nodes, before it's
  released.
- Contributors can't tell whether their change broke something.

## If we revisit this

What had been checked by hand is the obvious place to start:
- **Unit tests for the conflict rules** (`Vv::compare`, `decide`), so that both
  sides of a conflict pick the same winner.
- **A multi-node test.** Start several daemons on one machine, each with its own
  `HOME` and port. Check the first sync, edits in both directions, deletes, a
  file replaced the way editors save it, a node joining through another node,
  concurrent edits (one conflict copy, the same name on every node), and removing
  a path.
- **A GitHub Actions job** on a macOS runner that runs both on every push.
