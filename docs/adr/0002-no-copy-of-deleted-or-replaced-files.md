# ADR 0002: No copy is kept of files a peer deletes or replaces

- Status: Accepted (risk accepted, no change)
- Date: 2026-09-27

## Context

When a peer's change wins, the daemon applies it to disk and throws the old
version away (`Daemon::apply`, `src/daemon.rs:588`):

- **A deletion** calls `std::fs::remove_file` (`src/daemon.rs:599`).
- **An update** renames the downloaded file over the old one (`src/daemon.rs:621`).

The only version kept is the conflict copy, made when two nodes change the same
file at the same time (`src/daemon.rs:613`).

So one mistake on one machine, such as `rm -rf ~/notes` or saving an empty file,
spreads to every node within a second or two, and the old data is gone
everywhere.

## Decision

Accept this risk and keep the current behaviour: rust-sync keeps no copy of
files it deletes or replaces because of a peer's change.

## Consequences

- rust-sync is not a backup. Synced machines copy each other's mistakes, so
  another backup (for example Time Machine) is still needed.
- No extra disk space is used, and no trash needs cleaning up.
- The README's Limits section says so: "It's not a backup."

## If we revisit this

The fix considered was to move each old version into `~/.rust-sync/trash/`
before the daemon deletes or overwrites it, and to remove trash entries after
30 days. If the old version couldn't be kept, the change wouldn't be applied.

These were rejected:
- **The macOS Trash.** "Put Back" doesn't work for files moved there this way,
  and the Trash can be emptied at any time.
- **A hidden folder in each synced folder,** like Syncthing's `.stversions`. It
  clutters the parent folder of synced single files.

Keeping several versions of each file, with limits on count and size, was noted
as a later extension.
