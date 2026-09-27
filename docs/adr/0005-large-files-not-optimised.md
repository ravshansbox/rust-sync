# ADR 0005: Large files are sent whole and hashed on the main thread

- Status: Not planned yet
- Date: 2026-09-27

## Context

Two things make large files slow.

- **The whole file is sent on every change.** When a peer asks for a file,
  `upload` (`src/daemon.rs:1032`) reads it from the start and sends all of it, in
  32 KiB chunks (`CHUNK`, `src/net.rs:24`). Changing one byte in a 2 GB file sends
  2 GB again.
- **Hashing blocks everything else.** `check_file` hashes a changed file with
  `index::hash_file` (`src/daemon.rs:540`, `src/index.rs:142`) on the daemon's
  single event-loop thread. While it hashes a large file, the daemon handles no
  network messages, watcher events or CLI requests. Uploads and downloads pause,
  and `rust-sync status` waits.

This doesn't affect small files such as notes, config files and source code,
which is the main use today.

## Decision

Not planned yet. Keep sending whole files, and keep hashing on the main thread.

## Consequences

- Syncing large files that change often (disk images, virtual machines,
  databases, video projects) is slow and uses a lot of bandwidth.
- While a large file is being hashed, the daemon doesn't respond. Peers may see a
  short pause, and if hashing takes longer than 95 seconds, they drop the
  connection and reconnect.
- The README's Limits section says so: "Large files are slow."

## If we plan this

- **Hashing:** move it off the event loop into a blocking thread
  (`tokio::task::spawn_blocking`), with the result coming back as an event. This
  is the smaller change, and it fixes the pauses.
- **Transfers:** send only the changed parts. Split each file into blocks, hash
  each block, and have the receiver ask only for blocks it doesn't have. Syncthing
  uses fixed-size blocks; rsync uses a rolling checksum that also finds data that
  has moved. Either way, the index has to store block hashes, which changes the
  message format.
