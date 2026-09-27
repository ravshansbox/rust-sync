# rust-sync

Keeps chosen files and folders the same on several machines. Change a file on any
machine and the others get the change within a second or two. You can sync single
files, not only folders.

Built for macOS. It uses about 8–10 MB of RAM and no CPU when nothing changes.

## Quick start

On every machine:

```sh
cargo build --release
./target/release/rust-sync daemon        # keep this running
```

On machine A:

```sh
rust-sync add ~/notes                    # a folder
rust-sync add ~/.zshrc                   # or a single file
rust-sync node add macmini.local         # shows B's ID and asks you to confirm
```

On machine B:

```sh
rust-sync node add macbook.local         # B must trust A too
```

That's it. B now syncs `~/notes` and `~/.zshrc`. The list of synced paths is
shared, so `rust-sync add` on any machine applies everywhere.

To add a third machine C, pair it with any one machine (both directions). The
others learn about C from that machine and connect to it by themselves.

## Commands

| Command | Alias | What it does |
|---|---|---|
| `rust-sync daemon [--port N]` | | Run the sync process (default port 21987) |
| `rust-sync add <path>` | | Start syncing a file or folder |
| `rust-sync remove <path>` | `rm` | Stop syncing it. Files stay on disk |
| `rust-sync node add <host[:port]> [--id ID] [-y]` | | Trust a node |
| `rust-sync node remove <ID>` | | Stop trusting a node |
| `rust-sync status` | `list` | Nodes, paths, connections, pending requests |
| `rust-sync id` | | This node's ID |

If the daemon isn't running, `add`, `remove` and `node` edit the config file.
The changes take effect when the daemon starts.

## How it works

- **Watching:** macOS FSEvents via the `notify` crate. No polling. Changes are
  collected for 0.5 s (at most 3 s) before a file is read. A cheap full rescan
  (only checks size and mtime) runs every 15 minutes in case an event was missed.
- **Paths** under your home folder are shared as `~/…`, so they work when home
  folders differ between machines.
- **Versions:** each file has a BLAKE3 hash and a version vector (a counter per
  node). This tells a normal update apart from two machines editing the same
  file at the same time.
- **Conflicts:** the version with the newer mtime wins (an edit always beats a
  delete). The losing version is kept next to it as
  `name.sync-conflict-<time>-<node>.ext`.
- **Deletions** are recorded, so a machine that was offline doesn't bring a
  deleted file back.
- **Transfers:** whole files, in 32 KiB chunks, at most 8 at a time. Files are
  written to a temporary file, checked against the hash, then renamed into place.
  Mtime and permissions are copied.
- **Security:** each node has an X25519 key pair. Its public key is its node ID.
  Connections use the Noise XX handshake (the `snow` crate), so traffic is
  encrypted and both sides check each other's key. A node only talks to nodes it
  trusts.
- **Connecting:** every address a name resolves to is tried in parallel, with a
  new attempt every 250 ms (Happy Eyeballs, RFC 8305), and the first that
  connects is used. After a failed dial the node retries after 1 s, 2 s, 4 s…
  up to 30 s. It listens on IPv4 and IPv6.
- **State** is kept in `~/.rust-sync/` (`key`, `config.json`, `index.bin`). Set
  `RUST_SYNC_HOME` to use a different folder.

## macOS Local Network privacy

Since macOS 15, a process may need permission to connect to devices on your
Wi-Fi or Ethernet network (including a Thunderbolt Bridge). When it's blocked,
connections fail with `No route to host`, and the daemon logs a hint.

- **Exempt:** processes started from Terminal or over SSH (while that session
  is open), root processes, and launchd daemons.
- **Not exempt:** launchd *agents*, and a daemon started over SSH once that
  session has closed. We measured the second case.
- **VPN addresses are not "local network".** Using Tailscale names (for example
  `rust-sync node add macmini`) avoids the problem. This is the recommended setup.

Source: Apple, [TN3179: Understanding local network privacy](https://developer.apple.com/documentation/technotes/tn3179-understanding-local-network-privacy).

## Limits

- **Trust is transitive.** A trusted node can introduce new nodes and add paths
  to sync, including paths outside your home folder. Only pair machines you control.
- **Whole-file transfers.** A small change to a large file sends the whole file again.
- **Hashing is on the main thread.** While a very large file is being hashed,
  network traffic waits.
- **Not synced:** empty folders, symlinks, extended attributes. Temporary files
  (`*.rsync-tmp`) and `.DS_Store` are ignored.
- **Every node connects to every other node.** This is fine for a handful of machines.
- **No start-at-login yet.** Run the daemon yourself, or add a launchd agent.
