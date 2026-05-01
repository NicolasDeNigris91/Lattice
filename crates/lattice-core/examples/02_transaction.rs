//! Transactional read-modify-write with snapshot isolation and
//! per-key conflict detection. Demonstrates that a transaction
//! sees a consistent snapshot of the database for the duration
//! of its closure, and that the commit aborts with
//! [`Error::TransactionConflict`] if another writer modified one
//! of the transaction's keys between snapshot and commit.
//!
//! Run with:
//!
//! ```bash
//! cargo run -p lattice-core --example 02_transaction
//! ```

use lattice_core::{Error, Lattice, Result};
use std::thread;
use tempfile::tempdir;

fn parse_balance(bytes: Option<&[u8]>) -> i64 {
    bytes
        .and_then(|b| String::from_utf8_lossy(b).parse::<i64>().ok())
        .unwrap_or(0)
}

fn render(bytes: Option<&[u8]>) -> String {
    bytes.map_or_else(
        || "<absent>".to_string(),
        |b| String::from_utf8_lossy(b).into_owned(),
    )
}

fn main() -> Result<()> {
    let dir = tempdir().expect("create temp dir");
    let db = Lattice::open(dir.path())?;

    db.put(b"account:1", b"100")?;

    // Successful transaction: increment the balance by 25 atomically.
    db.transaction(|tx| {
        let current = parse_balance(tx.get(b"account:1")?.as_deref());
        tx.put(b"account:1", (current + 25).to_string().as_bytes());
        Ok::<_, Error>(())
    })?;
    println!(
        "after +25: balance = {}",
        render(db.get(b"account:1")?.as_deref()),
    );

    // Conflict demonstration: T1 reads, T2 commits a concurrent
    // write, T1 tries to commit and aborts. The application
    // pattern is "loop until commit succeeds with a fresh
    // snapshot".
    let db_t1 = db.clone();
    let db_t2 = db.clone();
    let t1 = thread::spawn(move || -> Result<i32> {
        let mut attempts = 0;
        loop {
            attempts += 1;
            let outcome = db_t1.transaction(|tx| {
                let current = parse_balance(tx.get(b"account:1")?.as_deref());
                // Yield long enough for T2 to slip in and force a
                // conflict on the first attempt.
                thread::sleep(std::time::Duration::from_millis(50));
                tx.put(b"account:1", (current + 10).to_string().as_bytes());
                Ok::<_, Error>(())
            });
            match outcome {
                Ok(()) => return Ok(attempts),
                Err(Error::TransactionConflict) => {}
                Err(other) => return Err(other),
            }
        }
    });

    // Brief sleep so T1 enters its closure first.
    thread::sleep(std::time::Duration::from_millis(20));
    let _ = db_t2.transaction(|tx| {
        let current = parse_balance(tx.get(b"account:1")?.as_deref());
        tx.put(b"account:1", (current + 5).to_string().as_bytes());
        Ok::<_, Error>(())
    });

    let attempts = t1.join().expect("t1 panicked")?;
    println!(
        "T1 retried {attempts} times before its commit succeeded; \
         final balance = {}",
        render(db.get(b"account:1")?.as_deref()),
    );

    Ok(())
}
