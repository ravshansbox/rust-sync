# ADR 0003: Exchange a protocol version during the handshake

- Status: Proposed
- Date: 2026-09-27

## Context

Messages between nodes are encoded with `postcard` (`src/net.rs`). Postcard
isn't self-describing. If one side adds a field, adds a message type, or reorders
anything, the other side misreads the bytes or fails with a decode error.

Nothing in the connection says which version is talking:

- The Noise handshake messages carry empty payloads (`hs.write_message(&[], …)`,
  `src/net.rs:161`).
- The first application message, `Hello { port, probe, trusts_you }`
  (`src/net.rs:30`), has no version either, and it is itself a postcard message
  whose layout may change.

So after the first change to the message format, a node running an old version
and a node running a new one fail with a confusing error such as "early eof" or a
decode failure, instead of "please upgrade".

This has to be fixed before the first public release. Adding a version later would
itself be a breaking change, and it would need exactly the check it adds.

## Decision

1. **Each side puts a fixed version record in its handshake payload.** Noise XX
   lets message 2 (from the responder) and message 3 (from the initiator) carry an
   encrypted, authenticated payload, so the version is known before any
   application message is read.
2. **The record has a fixed layout that never changes:** the bytes `rsync`, then
   a 2-byte protocol number (big-endian), then the app version as a short UTF-8
   string (`CARGO_PKG_VERSION`). It doesn't use postcard, so later format changes
   can't affect it.
3. **The protocol number starts at 1** and increases on every change that older
   nodes can't read. Changes that don't affect the bytes on the wire (bug fixes,
   new CLI commands) don't change it.
4. **Nodes connect only if their protocol numbers are equal.** Otherwise both
   sides close the connection and log a clear message naming the other node, both
   protocol numbers and both app versions, and saying which side should upgrade.
   `rust-sync node add` shows the same message.
5. **An empty payload means protocol 0,** which is what version 0.1.0 sends. A
   new node therefore recognises an old one and says "upgrade that node". An old
   node reads the payload but ignores it, and the new node closes the connection
   first.
6. **`rust-sync node list` shows each connected node's app version,** so a
   mismatch is easy to spot before it causes trouble.
7. **The crate version goes to 0.2.0** with this change.

## Alternatives considered

- **Put the version in the Noise prologue.** A mismatch would make the handshake
  fail, but only as a decryption error, with no way to say which version the
  other side runs.
- **Add a version field to `Hello`.** `Hello` is a postcard message, so the
  version check would depend on the format it's meant to protect.
- **Negotiate a range** (each side sends the lowest and highest version it
  supports, and they use the highest in common). This allows mixed versions
  during an upgrade, but every node then has to keep old message formats working.
  That isn't worth it for a few machines one person upgrades together. The record
  in point 2 leaves room to add a range later.
- **Switch to a self-describing format** (JSON, or a schema format with optional
  fields). Additive changes would stop breaking things, but messages get larger
  and slower, and changes that remove or rename fields would still need a version.

## Consequences

- A version mismatch gives a clear, actionable message instead of a confusing
  error.
- All nodes must run the same protocol version to sync. Upgrading one machine
  stops it syncing with the others until they're upgraded too. The log says so.
- This change is itself incompatible with 0.1.0. That's acceptable now, because
  the only nodes running 0.1.0 are the author's own.
