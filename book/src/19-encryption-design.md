# Encryption at rest: v2.0 design doc

This chapter is the design pass for encryption at rest, the
v2.0 milestone listed in chapter 8's deferral table and
sketched in chapter 15. It is intentionally written before
any code lands so the on-disk format change, the cipher
choice, the threat model, and the test contract are settled
in one place. The implementation phases at the end are the
ship plan; each phase has a green-bar gate before the next
starts.

The decisions captured here lock in three open questions the
chapter-15 sketch left dangling: the cleartext-to-encrypted
upgrade policy, the authenticated-additional-data binding,
and the test fence shape. They are no longer open.

## Threat model

Encryption at rest is not a one-line feature. The threat
model says exactly which adversary the implementation
defends against and which it does not. Documenting both
saves the next reader from assuming a guarantee Lattice does
not make.

**In scope:**

- An attacker who reads the bytes of a stopped database
  directory (forensic recovery off a discarded drive,
  backup tape, snapshot of a stopped container's volume)
  and wants the plaintext keys and values. Without the
  key, every byte the engine wrote is indistinguishable
  from random.
- An attacker who tampers with bytes on disk and wants the
  engine to deserialize the modified ciphertext as a
  different valid record. Tampering with any byte in an
  encrypted block, the WAL, or the manifest causes the
  Poly1305 authenticator to reject the record on read.
- An attacker who swaps an encrypted SSTable block for a
  different valid encrypted block from the same key. Bound
  via authenticated-additional-data (AAD) on
  `(sstable_seq, block_index)`; see [AAD binding][aad].

**Out of scope:**

- An attacker with a memory dump of a running process. The
  decrypted memtable, the decrypted block cache (when v2.x
  ships one), and the cipher's expanded key state all live
  in process memory and are recoverable from a heap dump.
  The mitigation lives outside the storage engine
  (mlock'd pages, hardware enclaves, full-disk swap
  encryption, OS-level memory protection); none of those
  are this engine's job.
- An attacker with the key. Encryption-at-rest does not
  defend against a key compromise. Key rotation is the
  user's responsibility (see [Out of scope][oos]).
- An attacker who can predict random nonces from a flawed
  RNG. Lattice uses `getrandom`-backed CSPRNG bytes from
  `rand_core`; the engine assumes the OS RNG is sound.
  When it is not, every cryptographic crate in the
  ecosystem breaks the same way.
- A timing-based side channel against a busy host. The
  cipher implementation is constant-time inside its
  primitives; the surrounding I/O patterns (block sizes,
  number of blocks read, compaction frequency) leak
  information to an attacker watching the disk. Workloads
  whose access pattern is itself secret need a different
  layer (oblivious storage, ORAM); Lattice does not pretend
  to provide that.

[aad]: #aad-binding
[oos]: #out-of-scope-non-goals

## Cipher choice

The cipher is **XChaCha20-Poly1305** as defined in
[RFC 7539 (ChaCha20-Poly1305)][rfc7539] with the XChaCha20
extended-nonce variant from
[draft-arciszewski-xchacha-03][xchacha].

[rfc7539]: https://datatracker.ietf.org/doc/html/rfc7539
[xchacha]: https://datatracker.ietf.org/doc/html/draft-arciszewski-xchacha-03

Why this and not AES-GCM, AES-OCB3, ChaCha20-Poly1305 with a
12-byte nonce, or anything else:

| candidate | nonce | objection |
|---|---|---|
| AES-128-GCM | 96 bits | Repeated nonces under a single key are catastrophic. With 96 bits the birthday bound bites at about 2^32 records under one key, which a long-lived database will exceed. |
| AES-256-GCM-SIV | 96 bits | Misuse-resistant against nonce reuse, but the nonce-reuse failure mode degrades to "leak whether two ciphertexts are equal" rather than to a formally indistinguishable encryption. We prefer to pick a cipher that simply does not need a counter discipline. |
| ChaCha20-Poly1305 (RFC 7539) | 96 bits | Same nonce-bound problem as AES-128-GCM. Acceptable for short-lived sessions; not for a database that may run for years. |
| AES-256-OCB3 | 96 bits | Patent encumbrance has lapsed but the implementation ecosystem in pure Rust is thinner than for ChaCha-family ciphers. Risk-adjusted, the win is small. |
| **XChaCha20-Poly1305** | **192 bits** | Random 192-bit nonce per call has a birthday bound at about 2^96. Effectively safe under any key for any realistic write volume. Pure-Rust implementation in `chacha20poly1305` is audited. AES-NI is not required, so the engine's perf does not depend on the host CPU. |

Performance on modern x86-64 with AES-NI: AES-256-GCM is
roughly 4 GiB/s per core; XChaCha20-Poly1305 is roughly
1 GiB/s per core. On a host without AES-NI (older ARM,
locked-down VMs), the picture inverts and ChaCha is faster.
Lattice's bottleneck is rarely the cipher; the WAL `fsync`
and the SSTable block I/O dominate. Trading a constant
~3x cipher-throughput for a permanent escape from the
nonce-counter discipline is the right call.

## On-disk format

Encryption requires an on-disk format bump. The shape is
designed so that:

1. A pre-v2 database opens as cleartext on a v2 binary, no
   key required.
2. A v2 database written with `encryption_key(...)` carries
   a flag bit in the SSTable footer and the WAL header.
3. A v2 binary opening a v2-encrypted directory without the
   key fails loudly with `Error::EncryptionKeyMissing`,
   never silently mis-decodes.
4. A v1 binary opening a v2 directory fails loudly with
   the existing "unsupported format version" error from
   the SSTable / manifest decoders.

### SSTable footer (format version 3)

The current footer (chapter 3, format version 2) is 48 bytes:

```text
| bloom_offset: u64 | bloom_length: u64 | index_offset: u64 |
| index_length: u64 | magic: u64        | version: u32      |
| reserved: u32     |
```

Format version 3 reuses the `reserved: u32` field as a
flags word. Bit 0 set means "blocks and index are
encrypted; the per-block nonce is the deterministic
derivation `nonce_prefix(8) || sstable_seq.to_le_bytes()(8) || block_index.to_le_bytes()(8)`".

```text
| bloom_offset: u64 | bloom_length: u64 | index_offset: u64 |
| index_length: u64 | magic: u64        | version: u32 = 3  |
| flags: u32        |
```

Where `flags` is a bitfield:

```text
bit 0: encrypted_blocks
bits 1..31: reserved (must be zero on read; allows future flags)
```

The format version stays self-describing. A v2 reader
opening a v3 footer with `encrypted_blocks` set demands the
key.

### Per-block layout (encrypted)

Encrypted blocks gain a fixed 16-byte authenticator tag at
the end. The block's pre-cipher bytes are the existing
data-block payload from chapter 3; the cipher operates on
the raw payload, not the LZ4-compressed bytes. Compression
runs first (so the ciphertext does not waste entropy on
repeated patterns), then encryption wraps the compressed
bytes.

```text
encrypted block on disk:

| ciphertext (compressed_len bytes) | poly1305_tag (16 bytes) |
```

The `compressed_len` field in the index is the **ciphertext
length only**, not including the tag. The reader knows to
fetch `compressed_len + 16` bytes when the footer flag is
set. This keeps the index entry layout unchanged.

### WAL header

The WAL's per-record framing today is `len: u32 | crc32: u32 | bincode_payload`. Format version 2 (encrypted)
prepends a 24-byte XChaCha nonce per record:

```text
encrypted WAL record:

| nonce: [u8; 24] | len: u32 | ciphertext + poly1305_tag (len + 16 bytes) |
```

The CRC32 is dropped under encryption; Poly1305 already
authenticates the bytes, and CRC over ciphertext is
load-carrying noise. The WAL global header gains a flag bit
distinguishing the two formats so replay knows whether to
expect nonces.

### Manifest

The manifest is a single small bincode record. Under
encryption, the on-disk file is `nonce(24) || ciphertext + tag`. The decoder reads
the nonce, decrypts the rest, and then runs the v2 bincode
parser on the plaintext. No format-version-flag bit is
needed because the manifest version field already advances:
v2 (current) cleartext, v3 encrypted-with-XChaCha20-Poly1305.

## AAD binding

Authenticated-additional-data (AAD) is the antidote to a
swapped-block attack. Without it, an attacker who controls
the disk could swap block 5 of SSTable 17 for block 2 of
SSTable 23 (encrypted with the same key), and the engine
would decrypt the substituted block successfully because
the cipher only authenticates the bytes, not the position.

The AAD binding for SSTable blocks is the byte sequence:

```text
AAD = "lattice-sst-block-v3" || sstable_seq (u64 le) || block_index (u64 le)
```

For WAL records:

```text
AAD = "lattice-wal-record-v2" || record_index (u64 le)
```

For the manifest:

```text
AAD = "lattice-manifest-v3"
```

The version-tagged context strings are domain separators:
they prevent an attacker from reusing an SSTable block's
ciphertext as a WAL record, or vice versa. The numeric
fields bind the ciphertext to its location.

The reader recomputes the same AAD before decrypting; if
any byte differs (the ciphertext was moved, the format
version was lied about), the Poly1305 verification fails
and `Error::EncryptionAuthenticationFailed` surfaces.

## Cleartext-to-encrypted upgrade

This was an open question in chapter 15. The decision is:

- **Default refuses.** Opening an unencrypted directory
  with a key set returns
  `Error::EncryptionMixedDirectory`.
- **Opt-in upgrade-on-write.** A new builder flag,
  `LatticeBuilder::allow_legacy_upgrade(true)`, permits
  opening the cleartext directory with a key. New
  SSTables and new WAL records are written encrypted; old
  SSTables and the existing WAL stay cleartext until the
  next compaction round rewrites them. The manifest tracks
  per-SSTable encryption status so reads dispatch to the
  right path.

The split makes the rare and risky path (mixed directories)
explicit at the call site. Most operators will reach for
the default and run a one-off offline `lattice
migrate-to-encrypted` command (a v2.x CLI subcommand) that
rewrites the directory in one pass, verifies the result,
and then cleans up the original.

## Performance budget

The cipher cost lands on the per-block path of every read
and every flush. The bench harness in `benches/put_get.rs`
gains an `encrypted_*` variant so the regression detector
on Bencher.dev tracks the cipher overhead independently of
the cleartext baseline.

Concrete budget (target hardware: x86-64 without AES-NI,
single core, 4 KiB blocks, in-memory FS):

| operation | cleartext budget | encrypted budget | overhead |
|---|---|---|---|
| `put` (durable, small value) | 12 µs | 14 µs | +17% |
| `put` (non-durable, small value) | 1.4 µs | 2.0 µs | +43% |
| `get` (warm, hits memtable) | 0.3 µs | 0.3 µs | 0% (no cipher work) |
| `get` (cold, hits SSTable block) | 28 µs | 32 µs | +14% |
| `flush` (1 MiB memtable) | 0.9 ms | 1.4 ms | +56% |
| `compact` (10 MiB merged) | 18 ms | 28 ms | +56% |

The numbers are predictions to be replaced with measurement
once the implementation lands; if any row exceeds the
budget by more than 25%, the implementation is wrong, not
the budget. The flush and compact rows are compute-bound;
the put rows are dominated by `fsync` and the cipher cost
is mostly visible on small non-durable writes where the
sync cost vanishes.

## Test contract

Three test fences land alongside the implementation:

1. **Round-trip property fence**:
   `encrypted_reopen_with_correct_key_returns_identical_state`.
   For 64 random op histories, the database's checksum
   (chapter 13) under cleartext open of a fresh directory
   must match the checksum under encrypted open + same ops
   + reopen with the same key.
2. **Wrong-key contract**:
   `encrypted_reopen_with_wrong_key_fails_loudly`. Opening
   an encrypted directory with the wrong key returns
   `Err(Error::EncryptionAuthenticationFailed)` on the
   first read; never silent corruption, never panic.
3. **Bit-flip fuzz**:
   `crates/lattice-core/fuzz/fuzz_targets/encrypted_block.rs`.
   Take a known-good encrypted SSTable block, flip one
   random bit anywhere in the ciphertext or tag, and
   assert the decrypt fails with
   `Error::EncryptionAuthenticationFailed`. No panic, no
   silent success. Same target for the WAL and the
   manifest.

The first two are integration tests in
`tests/encryption.rs`; the third is a `cargo-fuzz` target
alongside the existing decoder fuzzes.

## API sketch

The public surface is intentionally narrow:

```rust
// Sync.
let db = Lattice::builder(path)
    .encryption_key([0u8; 32])           // 32 bytes; the user's job to source.
    .open()?;

// Or with the legacy-upgrade opt-in.
let db = Lattice::builder(path)
    .encryption_key(key)
    .allow_legacy_upgrade(true)
    .open()?;

// Async mirror.
let db = AsyncLattice::builder(path)
    .encryption_key(key)
    .open()
    .await?;
```

`encryption_key` takes `[u8; 32]` by value. The builder
zeroizes the slot on drop (via `zeroize::Zeroize`) so a
panic during `open()` does not leave the key on the heap.
The runtime stores the expanded cipher state in
`Inner.cipher: Option<XChaCha20Poly1305>` (`None` for
unencrypted handles); reads dispatch on `Option::is_some`.

`Config` gains a `read-only` style boolean
`encryption_active: bool` so the runtime configuration
snapshot and the CLI `lattice config-show` reflect the
state without exposing the key bytes themselves.

New `Error` variants:

```rust
Error::EncryptionKeyMissing,             // open with key=None on encrypted dir
Error::EncryptionKeyUnexpected,          // open with key=Some(...) on cleartext dir
Error::EncryptionMixedDirectory,         // legacy upgrade not opted in
Error::EncryptionAuthenticationFailed,   // tag verify failed; tampering or wrong key
```

## Out of scope (non-goals)

These are deliberately not part of v2.0:

- **Key rotation**: the engine takes a single key for the
  lifetime of the open handle. Rotation is implemented by
  the application: open with the old key, dump via
  `backup_to`, close, open the backup with the new key,
  swap directories. A future v2.x release may add an
  online rotation primitive, but the v2.0 contract is one
  key per open.
- **Envelope encryption / KEK / DEK hierarchy**: out of
  scope. The user picks one 32-byte data key and supplies
  it directly. Wrapping with a KEK lives in the
  application layer (or in a sidecar like Vault, AWS KMS,
  GCP KMS).
- **Hardware enclaves**: out of scope. SGX / Apple
  Secure Enclave / TPM-backed key storage are platform
  concerns; the engine accepts the key as bytes and does
  not care where they came from.
- **Per-key encryption**: every record under a single
  Lattice handle is encrypted with the same data key.
  Per-namespace or per-tenant keys live above the storage
  layer.
- **At-rest plaintext leak audit**: the engine does not
  promise that no plaintext byte ever lands on a swap
  page or a tmpfs spool. Workloads requiring that
  guarantee need full-disk encryption underneath plus
  swap disabled or encrypted.

## Implementation phases

The ship plan is incremental. Each phase has a green-bar
gate (test suite passes, clippy strict, doc strict) before
the next phase touches code.

**Phase A: cipher plumbing without on-disk impact**
(~one session). Add `chacha20poly1305` and `zeroize`
dependencies. Build an internal `Cipher` wrapper that owns
the key, exposes `seal_block(plaintext, aad) -> Vec<u8>`
and `open_block(ciphertext, aad) -> Result<Vec<u8>>`, and
zeroizes on drop. Unit-tested in isolation. Public API
unchanged.

**Phase B: SSTable v3 with encrypted blocks**
(~two sessions). Wire the cipher through `SSTableWriter`
and `SSTableReader` behind a footer flag. Update the
existing v2 footer reader to recognise v3 footers and
demand the key. Pin the round-trip property fence and the
bit-flip fuzz target.

**Phase C: WAL v2 with encrypted records**
(~one session). Same shape as phase B for the WAL. Replay
correctness pinned by the existing
`arbitrary_ops_match_btreemap_after_reopen` property fence,
extended to run with both a cleartext and an encrypted
config.

**Phase D: manifest v3 + builder option**
(~one session). Encrypt the manifest, wire
`LatticeBuilder::encryption_key`, surface the four new
`Error` variants. Public API change: this is when the
v2.0.0 tag lands.

**Phase E: cleartext-to-encrypted migration**
(~one session). Add `LatticeBuilder::allow_legacy_upgrade`
and the `lattice migrate-to-encrypted` CLI subcommand. The
engine learns to read mixed directories (per-SSTable flag)
and rewrites cleartext SSTables on every compaction
round. Pinned by an integration test that mutates a
cleartext directory, opens it with `allow_legacy_upgrade`,
runs writes + compactions, and reopens to confirm every
SSTable is encrypted.

**Phase F: bench harness with encrypted variant**
(~half session). Mirror every benchmark in
`benches/put_get.rs` with an `_encrypted` suffix. Wire
both groups into the Bencher.dev panel so the regression
detector tracks cipher overhead per release.

Total scope: roughly six to seven sessions for v2.0.0,
plus a follow-up release for any items the fuzz harness
shakes out.

## Production readiness gate

The chapter 18 production-readiness matrix gains an
"Encryption at rest" row marked **shipped (v2.0)** when
phase D lands. The row points back to this chapter for the
threat model and out-of-scope list, and forward to the
fuzz harness and the bencher panel for the live evidence.

Until v2.0.0 ships, the row stays at "out of scope (v2.x)".
This chapter exists so a prospective user evaluating
Lattice today can see the design is settled and the gate
is concrete, even though the code is not yet written.
