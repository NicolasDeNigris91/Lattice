# Security policy

## Supported versions

Lattice is in the v1.x series. Security fixes land on the latest
minor release on `main`; older minors are not patched. The
project ships every release as a tagged commit and a published
crates.io artifact, so the upgrade path is always
`cargo update -p lattice-core` plus the relevant minor bump.

| Version  | Supported          |
| -------- | ------------------ |
| latest minor on `main` | yes |
| any prior release      | no  |

## Reporting a vulnerability

If you find a security issue, please open a private security
advisory on GitHub at
<https://github.com/NicolasDeNigris91/Lattice/security/advisories/new>
or email nicolas.denigris91@icloud.com with the subject prefixed
`[lattice security]`. Encrypted reporting is welcome; ask in the
first message and I will share a public key.

Please do not file a public issue for vulnerabilities. I will
acknowledge within seven days and aim to ship a fix or
mitigation within thirty days. CVEs are filed via GitHub
Security Advisories where applicable.

## Scope

Lattice is an embedded library. The threat model assumes:

- A trusted local process. The library does not sandbox calls
  from inside the same address space.
- A trusted local filesystem. On-disk file permissions are the
  caller's responsibility.
- A trusted at-rest medium. Encryption at rest is tracked as a
  v2.x feature (see book chapter 15); cleartext on disk is the
  v1.x baseline.

In scope:

- Memory safety bugs (the crate is `#![forbid(unsafe_code)]`,
  so any UB is dependency or std behaviour and gets reported
  upstream as well).
- Decoder bugs in the WAL, SSTable, or manifest parser that
  could cause a panic or out-of-bounds read on a malformed
  on-disk file. These are exercised by the cargo-fuzz targets
  in `crates/lattice-core/fuzz/`.
- Compaction or recovery bugs that could lose acknowledged
  durable writes. The four-pillar property fence
  (replay-on-reopen, snapshot isolation, compaction
  equivalence, transaction rollback semantics) and the loom
  suite under `lattice-loom-tests` are the primary defences.

Out of scope:

- Network exposure (Lattice does not open sockets).
- Multi-tenant isolation (the engine has no concept of users).
- Side-channel resistance (timing or cache attacks).
- Denial-of-service via crafted call sequences from a trusted
  caller (e.g. unbounded scan, oversized values).
- Cleartext on-disk data prior to v2.x encryption-at-rest.
