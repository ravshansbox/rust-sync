# ADR 0003: Nodes don't exchange a protocol version

- Status: Accepted (risk accepted, no change)
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

## Decision

Accept this risk and keep the current protocol, with no version.

## Consequences

- All nodes must run the same rust-sync version. After an upgrade that changes
  the message format, upgrade every machine, or they stop syncing with confusing
  errors in the log.
- The README's Limits section says so: "Every node must run the same version."
- Adding a version later will itself be a breaking change. Nodes without it
  can't talk to nodes with it, so every node must be upgraded at the same time
  once.

## If we revisit this

The fix considered, and tested against a real 0.1.0 node before being dropped,
was:
- Each side puts a fixed record in its encrypted Noise handshake payload
  (messages 2 and 3): the bytes `rsync`, a 2-byte protocol number and the app
  version.
- Nodes connect only if the protocol numbers match. Otherwise both log which
  node needs upgrading.
- An empty payload means an old node.

These were rejected:
- **The Noise prologue.** A mismatch shows up only as a decryption error.
- **A version field in `Hello`.** It depends on the format it's meant to protect.
- **Negotiating a range of versions.** It isn't worth it for a few machines
  upgraded together.
- **A self-describing format.** Larger messages, and it still needs a version
  for removed or renamed fields.
