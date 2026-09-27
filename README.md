# rust-sync

Keeps chosen files and folders the same on several machines. Change a file on any
machine and the others get the change within a second or two. You can sync single
files, not only folders.

Built for macOS. It uses about 8–10 MB of RAM and no CPU when nothing changes.

## Install

You need [Rust](https://rustup.rs). On every machine:

```sh
cargo install --locked --force --git https://github.com/ravshansbox/rust-sync
rust-sync service install                # start now and at every login
```

This puts `rust-sync` in `~/.cargo/bin`. `--force` makes cargo rebuild even if
this version is already installed, so the same command also upgrades or
reinstalls. To run the daemon in a terminal instead, use `rust-sync daemon`.

**Upgrade or reinstall:** run both commands again. The second one restarts the
daemon on the new binary.

**Uninstall:**

```sh
rust-sync service uninstall
cargo uninstall rust-sync
rm -r ~/.rust-sync                       # optional: node key, config and index
```

## Quick start

On machine A:

```sh
rust-sync path add ~/notes               # a folder
rust-sync path add ~/.zshrc              # or a single file
rust-sync node add macmini               # shows B's ID and asks you to confirm
```

On machine B:

```sh
rust-sync node add macbook               # B must trust A too
```

Use a hostname, Tailscale name or IP address. See
[macOS Local Network privacy](#macos-local-network-privacy) for why Tailscale
names are the safest choice on macOS.

That's it. B now syncs `~/notes` and `~/.zshrc`. The list of synced paths is
shared, so `rust-sync path add` on any machine applies everywhere.

To add a third machine C, pair it with any one machine (both directions). The
others learn about C from that machine and connect to it by themselves.

## Commands

| Command | Alias | What it does |
|---|---|---|
| `rust-sync daemon [--port N]` | | Run the sync process (default port 21987) |
| `rust-sync path add <path>` | | Start syncing a file or folder |
| `rust-sync path list` | `path ls` | Synced files and folders |
| `rust-sync path remove <path>` | `path rm` | Stop syncing it. Files stay on disk |
| `rust-sync node add <host[:port]> [--id ID] [-y]` | | Trust a node |
| `rust-sync node list` | `node ls` | Trusted nodes, their addresses and connection state |
| `rust-sync node remove <ID>` | `node rm` | Stop trusting a node |
| `rust-sync service install` | | Start the daemon at login (and now) |
| `rust-sync service uninstall` | | Stop it and stop starting it at login |
| `rust-sync status` | | Nodes, paths, connections, pending requests |
| `rust-sync id` | | This node's ID |

If the daemon isn't running, `path add`, `path remove`, `node add` and
`node remove` edit the config file.
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

## Start at login

```sh
rust-sync service install
```

This installs a per-user launchd agent
(`~/Library/LaunchAgents/com.github.ravshansbox.rust-sync.plist`) that runs
`rust-sync daemon` at login and restarts it if it crashes. The log is in
`~/Library/Logs/rust-sync.log`. Run `service install` again after moving or
upgrading the binary. If you build from source, copy the binary somewhere stable
first, because the agent runs it from where it was when you installed. `rust-sync service uninstall` removes the agent and leaves your config
and files alone.

A launchd agent is not exempt from Local Network privacy (below). Use Tailscale
names for nodes, or allow the prompt if macOS shows one.

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
- **Logs have no timestamps**, and the log file isn't rotated.
