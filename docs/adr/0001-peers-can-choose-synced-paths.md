# ADR 0001: Trusted peers can choose which paths are synced

- Status: Accepted (risk accepted, no change)
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

## Decision

Accept this risk and keep the current behaviour: every trusted node can add paths
for all nodes.

rust-sync is currently used only between machines that one person owns and
controls. There, a path added on one machine is meant to sync everywhere.

## Consequences

- Trusting a node means trusting it with any file your user account can read
  and write. Only pair machines you control.
- The README's Limits section already says so: "Trust is transitive. A trusted
  node can introduce new nodes and add paths to sync, including paths outside
  your home folder."
- Adding a path on one machine keeps spreading to all nodes by itself, with no
  extra step.

## If we revisit this

The fix considered was to make the shared list a list of offers. Each node would
keep its own set of accepted paths, and a new `rust-sync path accept <path>`
command would accept a path another node offered. The daemon would only act on
accepted paths.

These were rejected as incomplete:
- **Only allowing paths under the home folder.** `~/.ssh` and `~/.zshrc` are
  inside it.
- **A list of forbidden paths.** It can never be complete.
- **Stopping nodes from introducing each other.** A node you paired with could
  still choose your paths.
