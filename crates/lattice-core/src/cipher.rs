//! Authenticated-encryption wrapper for the v2.0 encryption-at-rest
//! milestone, phase A.
//!
//! INVARIANT: a [`Cipher`] constructed from a 32-byte key seals
//! plaintext into ciphertext + Poly1305 tag and rejects any byte
//! tampering (ciphertext, tag, or AAD) on the open path with
//! [`CipherError::Authentication`]. The key bytes are wrapped in a
//! [`zeroize::Zeroizing`] container so a panic anywhere on the
//! seal/open path does not leave key material on the heap.
//!
//! This module is crate-private and not part of the public API
//! contract; the public `LatticeBuilder::encryption_key` knob
//! lands with phase D (book chapter 19). Phase A's job is to
//! land the cipher plumbing in isolation, so phases B-D have a
//! tested primitive to build against.
//!
//! Cipher: [XChaCha20-Poly1305][xchacha], picked for its 192-bit
//! nonce. A random nonce per call has a birthday bound at about
//! 2^96, which is effectively safe under any key for any
//! realistic write volume; the 12-byte-nonce alternatives
//! (AES-128-GCM, RFC 7539 ChaCha20-Poly1305) would put the
//! engine on a per-key counter discipline incompatible with a
//! long-lived database.
//!
//! [xchacha]: https://datatracker.ietf.org/doc/html/draft-arciszewski-xchacha-03

// The whole module is build-ahead-of-use scaffolding for the
// v2.0 milestone. Phase A ships the primitive and its unit
// tests; phases B-D wire it through the SSTable, WAL, and
// manifest paths and pull these symbols into use. Until then
// the items here look "unused" from the non-test build, even
// though every public symbol is exercised by the
// `#[cfg(test)]` block at the bottom of the file.
#![allow(dead_code)]

use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{Key, XChaCha20Poly1305, XNonce};
use zeroize::Zeroizing;

/// Length of the cipher's key in bytes. XChaCha20-Poly1305 is
/// always 256 bits.
pub(crate) const KEY_LEN: usize = 32;

/// Length of the cipher's nonce in bytes. `XChaCha20` uses a
/// 192-bit (24-byte) nonce; this is the field's load-bearing
/// difference from RFC 7539 `ChaCha20-Poly1305`.
pub(crate) const NONCE_LEN: usize = 24;

/// Length of the Poly1305 authenticator tag appended to every
/// sealed payload.
pub(crate) const TAG_LEN: usize = 16;

/// Errors surfaced by the cipher wrapper. The crate-level
/// [`crate::Error`] type will gain `Encryption*` variants in
/// phase D that map from these; phase A keeps the cipher's
/// failure mode local so it can be unit-tested without going
/// through the larger error enum.
#[derive(Debug, thiserror::Error)]
pub(crate) enum CipherError {
    /// The Poly1305 verification failed. Either the ciphertext
    /// (or tag, or AAD) was tampered with, or the wrong key is
    /// trying to open the payload. Indistinguishable from the
    /// outside, by design.
    #[error("authentication failed: ciphertext, tag, or AAD does not verify")]
    Authentication,
}

/// Result alias for cipher operations.
pub(crate) type CipherResult<T> = std::result::Result<T, CipherError>;

/// Authenticated-encryption wrapper holding a 32-byte data key.
/// Keep one per open `Lattice`; the cipher itself is `Send +
/// Sync` because XChaCha20-Poly1305 has no per-call mutable
/// state (the underlying `aead::Aead` impl is reentrant).
///
/// The key bytes live behind [`Zeroizing`] so the heap memory
/// is wiped on drop. A panic anywhere on a seal/open call
/// unwinds through `Drop`, which means even an incidentally
/// dropped `Cipher` cannot leak its key.
pub(crate) struct Cipher {
    /// Expanded XChaCha20-Poly1305 state. The crate exposes
    /// internal zeroization on its `Drop`, so wrapping the
    /// `Cipher` itself in `Zeroizing` is unnecessary; we keep
    /// the original key bytes alongside under `Zeroizing` so
    /// `Debug` and equality comparisons stay zeroize-aware.
    aead: XChaCha20Poly1305,
    /// Pinned copy of the raw key bytes. Not used by the seal /
    /// open path (the `XChaCha20Poly1305` carries its own
    /// expanded state); held here so `Drop` zeroizes the
    /// material even if the upstream cipher implementation
    /// changes how it wipes its internals. Belt and braces.
    _key: Zeroizing<[u8; KEY_LEN]>,
}

impl std::fmt::Debug for Cipher {
    /// Never reveal the key bytes through `Debug`. Reads as a
    /// stable string so logs and panic backtraces print the
    /// type name without leaking material.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Cipher { /* redacted */ }")
    }
}

impl Cipher {
    /// Construct a cipher from raw key bytes. The constructor
    /// is infallible: `XChaCha20Poly1305::new` accepts every
    /// 32-byte sequence as a valid key, so there is no error
    /// path to surface.
    pub(crate) fn new(key: [u8; KEY_LEN]) -> Self {
        let pinned = Zeroizing::new(key);
        let aead = XChaCha20Poly1305::new(Key::from_slice(pinned.as_ref()));
        Self { aead, _key: pinned }
    }

    /// Seal `plaintext` under `nonce` and `aad`, returning a
    /// fresh `Vec<u8>` whose layout is `ciphertext || tag`.
    /// The caller is responsible for nonce generation and for
    /// transporting the nonce alongside the ciphertext (the
    /// cipher does not embed it).
    ///
    /// Panics: never. The underlying `aead::Aead::encrypt`
    /// returns `Result`, but XChaCha20-Poly1305 only fails
    /// when the plaintext is so large the resulting ciphertext
    /// would overflow `u64` (about 256 GiB), which Lattice
    /// blocks and WAL records do not approach. We map the
    /// hypothetical failure to a panic to keep the signature
    /// simple at this layer; phase D plumbs it through the
    /// crate's `Error` if a code path ever needs the safer
    /// shape.
    pub(crate) fn seal(&self, nonce: &[u8; NONCE_LEN], aad: &[u8], plaintext: &[u8]) -> Vec<u8> {
        let nonce = XNonce::from_slice(nonce);
        self.aead
            .encrypt(
                nonce,
                Payload {
                    msg: plaintext,
                    aad,
                },
            )
            .expect("xchacha20poly1305 seal: payload size out of range")
    }

    /// Open `ciphertext_with_tag` under `nonce` and `aad`,
    /// returning the recovered plaintext on success. Returns
    /// [`CipherError::Authentication`] if Poly1305 verification
    /// fails for any reason (wrong key, tampered ciphertext,
    /// tampered tag, mismatched AAD); the error is deliberately
    /// indistinguishable across those cases.
    pub(crate) fn open(
        &self,
        nonce: &[u8; NONCE_LEN],
        aad: &[u8],
        ciphertext_with_tag: &[u8],
    ) -> CipherResult<Vec<u8>> {
        let nonce = XNonce::from_slice(nonce);
        self.aead
            .decrypt(
                nonce,
                Payload {
                    msg: ciphertext_with_tag,
                    aad,
                },
            )
            .map_err(|_| CipherError::Authentication)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(seed: u8) -> [u8; KEY_LEN] {
        let mut k = [0u8; KEY_LEN];
        for (i, b) in k.iter_mut().enumerate() {
            #[allow(clippy::cast_possible_truncation)]
            let v = (i as u8).wrapping_mul(31).wrapping_add(seed);
            *b = v;
        }
        k
    }

    fn nonce(seed: u8) -> [u8; NONCE_LEN] {
        let mut n = [0u8; NONCE_LEN];
        for (i, b) in n.iter_mut().enumerate() {
            #[allow(clippy::cast_possible_truncation)]
            let v = (i as u8).wrapping_mul(7).wrapping_add(seed);
            *b = v;
        }
        n
    }

    #[test]
    fn round_trip_returns_the_original_plaintext() {
        let cipher = Cipher::new(key(1));
        let nonce = nonce(1);
        let aad = b"lattice-sst-block-v3 || seq=42 || idx=0";
        let plaintext = b"the quick brown fox jumps over the lazy dog".as_slice();

        let sealed = cipher.seal(&nonce, aad, plaintext);
        // Sealed payload is ciphertext + 16-byte Poly1305 tag.
        assert_eq!(sealed.len(), plaintext.len() + TAG_LEN);

        let opened = cipher.open(&nonce, aad, &sealed).unwrap();
        assert_eq!(opened, plaintext);
    }

    #[test]
    fn ciphertext_does_not_equal_plaintext() {
        // Stream-cipher sanity: the encrypted bytes must not
        // expose the plaintext shape verbatim. (XChaCha is a
        // stream cipher, so this is a smoke check that the
        // wiring is real, not a security claim.)
        let cipher = Cipher::new(key(2));
        let plaintext = vec![0xAAu8; 64];
        let sealed = cipher.seal(&nonce(2), b"aad", &plaintext);
        assert_ne!(&sealed[..plaintext.len()], plaintext.as_slice());
    }

    #[test]
    fn wrong_key_fails_authentication() {
        let writer = Cipher::new(key(3));
        let reader = Cipher::new(key(4));
        let nonce = nonce(3);
        let aad = b"aad";
        let sealed = writer.seal(&nonce, aad, b"secret");

        let err = reader.open(&nonce, aad, &sealed).unwrap_err();
        assert!(matches!(err, CipherError::Authentication));
    }

    #[test]
    fn aad_mismatch_fails_authentication() {
        // AAD binding is the antidote to a swapped-block
        // attack: the reader recomputes the AAD from
        // (sstable_seq, block_index) and any mismatch fails
        // the verify, even with the right key and the right
        // ciphertext.
        let cipher = Cipher::new(key(5));
        let nonce = nonce(5);
        let sealed = cipher.seal(&nonce, b"aad-original", b"payload");

        let err = cipher.open(&nonce, b"aad-tampered", &sealed).unwrap_err();
        assert!(matches!(err, CipherError::Authentication));
    }

    #[test]
    fn nonce_mismatch_fails_authentication() {
        let cipher = Cipher::new(key(6));
        let sealed = cipher.seal(&nonce(6), b"aad", b"payload");

        let err = cipher.open(&nonce(7), b"aad", &sealed).unwrap_err();
        assert!(matches!(err, CipherError::Authentication));
    }

    #[test]
    fn flipping_a_ciphertext_bit_fails_authentication() {
        // Bit-flip fuzz contract from book chapter 19: an
        // attacker who flips a single bit anywhere in the
        // sealed payload (ciphertext or tag) must not get a
        // silent decode success.
        let cipher = Cipher::new(key(7));
        let nonce = nonce(7);
        let aad = b"aad";
        let plaintext = b"important payload";
        let mut sealed = cipher.seal(&nonce, aad, plaintext);

        // Flip every bit of every byte in turn. None should
        // produce a successful open.
        for byte_idx in 0..sealed.len() {
            for bit in 0..8 {
                sealed[byte_idx] ^= 1 << bit;
                assert!(
                    cipher.open(&nonce, aad, &sealed).is_err(),
                    "tampering at byte {byte_idx} bit {bit} should fail",
                );
                sealed[byte_idx] ^= 1 << bit;
            }
        }

        // After restoring every bit, the original payload
        // still opens cleanly.
        let opened = cipher.open(&nonce, aad, &sealed).unwrap();
        assert_eq!(opened, plaintext);
    }

    #[test]
    fn truncated_ciphertext_fails_authentication() {
        let cipher = Cipher::new(key(8));
        let nonce = nonce(8);
        let aad = b"aad";
        let sealed = cipher.seal(&nonce, aad, b"payload");

        // Drop the last byte (corrupts the tag).
        let truncated = &sealed[..sealed.len() - 1];
        assert!(cipher.open(&nonce, aad, truncated).is_err());
    }

    #[test]
    fn debug_does_not_leak_key_bytes() {
        let cipher = Cipher::new(key(9));
        let s = format!("{cipher:?}");
        assert!(s.contains("redacted"));
        assert!(!s.contains("Key"));
    }

    #[test]
    fn empty_plaintext_round_trips() {
        // Edge case: a zero-length payload still produces a
        // valid 16-byte tag and verifies on open.
        let cipher = Cipher::new(key(10));
        let nonce = nonce(10);
        let sealed = cipher.seal(&nonce, b"aad", b"");
        assert_eq!(sealed.len(), TAG_LEN);
        let opened = cipher.open(&nonce, b"aad", &sealed).unwrap();
        assert!(opened.is_empty());
    }
}
