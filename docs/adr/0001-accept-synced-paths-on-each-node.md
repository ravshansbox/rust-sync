# ADR 0001: Each node accepts synced paths itself

- Status: Proposed
- Date: 2026-09-27

## Context

The list of synced paths is shared by all nodes. When a node receives a peer's
list, it takes every newer entry as its own (`Config::merge`,
`src/config.rs:96`). It then starts watching those paths, sends their files to
peers, and writes the files peers send (`reconcile_watches`, `on_entry` and
`on_request` in `src/daemon.rs`).

The only path check is in `paths::to_local` (`src/paths.rs:36`): it rejects `..`
and rust-sync's own state folder, and accepts every other absolute path.

So any trusted node can add any path on every other node. That includes nodes you
never paired with yourself, because nodes introduce each other. This allows:

- **Reading your secrets.** A peer adds `~/.ssh`. Your node uploads your private
  keys to every node.
- **Running code on your machine.** A peer adds `~/.ssh/authorized_keys` or
  `~/.zshrc` and sends its own version. Your node writes it.

Trusting a node to sync with it should not mean trusting it to choose which of
your files are synced.

## Decision

A path is synced on a node only after someone on **that** node accepts it.

1. **The shared list becomes a list of offers.** Paths still spread to all nodes,
   so each node can see what the others offer. Each node also keeps a local set of
   accepted paths, in a new file (`~/.rust-sync/accepted.json`) that is never sent
   to peers.
2. **`rust-sync path add <path>` accepts the path on this node** and offers it to
   the others, as today.
3. **New command: `rust-sync path accept <path>`.** It accepts a path another node
   offered. The path doesn't need to exist yet, because it may be a folder this
   node doesn't have.
4. **The daemon only acts on accepted paths.** For a path that is offered but not
   accepted, it doesn't watch, upload, download, or create folders. Every check
   that uses `root_of` (watching, `on_fs`, `on_entry`, `on_request`) uses the
   accepted set instead.
5. **`rust-sync path list` shows both kinds:** accepted paths as "syncing", and
   offered paths as "offered by <node>", with the command to accept them. The
   node comes from the entry's stamp. The daemon also logs new offers.
6. **Removing still spreads.** `path remove` on any node stops that path on all
   nodes and drops it from their accepted sets. Stopping a sync is always safe, so
   it needs no approval.
7. **Upgrade:** the first time a node runs the new version, every path that is
   currently active is accepted automatically, so nothing stops syncing.

## Alternatives considered

- **Only allow paths under the home folder.** This blocks `/etc`, but `~/.ssh`
  and `~/.zshrc` are the most dangerous targets, and they're inside home.
- **A list of forbidden paths** (`~/.ssh`, shell start-up files, `~/Library`).
  It can never be complete, and it blocks users who really do want to sync one of
  those.
- **Stop nodes introducing each other.** This limits the problem to nodes you
  paired with yourself, but a node you paired with could still choose your paths.
  It also makes adding a third machine harder.

## Consequences

- Adding a path now takes one step on every node (`path add` on one, `path accept`
  on the others), instead of spreading by itself. This is the price of the fix.
- A peer can no longer read or write files you didn't choose.
- Introductions are still automatic. An introduced node can receive files from the
  paths you accepted. Whether introductions should also need approval is a
  separate decision, not made here.
