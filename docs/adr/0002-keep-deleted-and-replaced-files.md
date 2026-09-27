# ADR 0002: Keep files that a peer deletes or replaces

- Status: Proposed
- Date: 2026-09-27

## Context

When a peer's change wins, the daemon applies it to disk and throws the old
version away (`Daemon::apply`, `src/daemon.rs:588`):

- **A deletion** calls `std::fs::remove_file` (`src/daemon.rs:599`).
- **An update** renames the downloaded file over the old one (`src/daemon.rs:621`).

The only version kept is the conflict copy, made when two nodes change the same
file at the same time (`src/daemon.rs:613`).

So one mistake on one machine, such as `rm -rf ~/notes` or saving an empty file,
spreads to every node within a second or two, and the data is gone everywhere.
The machines copy each other, so they give no protection against this.

## Decision

Before the daemon deletes or replaces a file **because of a change from a peer**,
it moves the old version into rust-sync's trash.

1. **Where:** `~/.rust-sync/trash/`, laid out like the original paths, with the
   time added to each name. For example, `~/notes/todo.md` becomes
   `~/.rust-sync/trash/notes/todo.md.2026-09-27T12-00-00`. Paths outside the home
   folder go under `trash/_root/`. The state folder is never synced, so the trash
   doesn't spread to other nodes.
2. **How:** a rename, which is instant on the same volume. If the synced folder is
   on another volume, the rename fails; the daemon then copies the file and
   deletes the original. If keeping the old version fails, the daemon does not
   apply the change. It logs an error and tries again later. Losing the old
   version silently is not allowed.
3. **What:** only files the daemon itself deletes or overwrites. Your own local
   deletes and edits aren't copied. They're your actions, and on the machine where
   you deleted a file, the other nodes' trash holds its last version.
4. **For how long:** 30 days. Once a day the daemon removes trash entries older
   than that, and it logs how much it freed.
5. **Restoring:** you copy the file back by hand. `rust-sync status` shows the
   trash folder and its size. A `rust-sync trash` command can come later if it's
   needed.

## Alternatives considered

- **The macOS Trash (`~/.Trash`).** Users already know it. But Finder's "Put
  Back" only works for files deleted through Finder, the names clash, and the
  Trash can be emptied by other things at any time.
- **A hidden folder in each synced folder** (like Syncthing's `.stversions`).
  This keeps the trash on the same volume. But synced single files have no folder
  of their own, so it would clutter the parent folder, and those hidden folders
  would need excluding from sync.
- **Keep several versions of each file,** with limits on count and size. This is
  more useful, but more code. The age-based trash can be extended to this later.

## Consequences

- A mistaken delete or overwrite on one machine can be undone from any other
  machine for 30 days.
- The trash uses disk space: in the worst case, 30 days of every file a peer
  deleted or changed. A very active folder with large files could use a lot, and
  the size in `status` is there to show it.
- Updating a file on another volume costs an extra copy of the old version.
