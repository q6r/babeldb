//! babeldb embedded in another Rust program (the setup of the README section
//! "Usar em outro projeto"): fjall + write-through WAL, group-committed writes,
//! chat-style keys (channel, snowflake).
//!
//! Run: `cargo run --release --example embedded --features fjall -- <dir>`

use std::sync::Arc;

use babeldb::config::WalConfig;
use babeldb::scale::chat::{channel_prefix, message_key, parse_message_key};
use babeldb::scale::group_commit::{GroupCommitConfig, GroupCommitter};
use babeldb::{Config, Db, Expect, ScanOptions};

fn main() -> babeldb::Result<()> {
    let dir = std::env::args().nth(1).unwrap_or_else(|| "data/embedded-example".into());
    let db = Arc::new(Db::open_fjall_wal(&dir, Config::adaptive(), WalConfig::default())?);
    // Writes from many threads go through one committer: one durable commit per batch.
    let writer = Arc::new(GroupCommitter::new(db.clone(), GroupCommitConfig::default())?);

    let channel = 42u64;
    let threads: Vec<_> = (0..4u64)
        .map(|t| {
            let writer = Arc::clone(&writer);
            std::thread::spawn(move || -> babeldb::Result<()> {
                for i in 0..250u64 {
                    let id = t * 1_000 + i;
                    let payload = format!("{{\"author\":{t},\"content\":\"message {id}\"}}");
                    // Durable when it returns.
                    writer.put(message_key(channel, id).to_vec(), payload.into_bytes(), Expect::Any)?;
                }
                Ok(())
            })
        })
        .collect();
    for t in threads {
        t.join().expect("writer thread panicked")?;
    }

    let key = message_key(channel, 7);
    let value = db.get(&key)?.expect("message 7 was written");
    println!("message 7: {}", String::from_utf8_lossy(&value));

    // The 5 newest messages of the channel, newest first.
    let latest = db.scan(&ScanOptions::prefix(&channel_prefix(channel)).reverse(true).limit(5).with_values(true))?;
    for item in &latest {
        let (_, id) = parse_message_key(&item.key).expect("chat key");
        println!("latest: id {id}, {} bytes", item.logical_len);
    }

    // Compare-and-set: only write if the key does not exist yet.
    let created = writer.put(message_key(channel, 7).to_vec(), b"dup".to_vec(), Expect::Absent);
    println!("put Absent on an existing key: {}", if created.is_err() { "refused" } else { "written" });

    writer.delete(key.to_vec(), Expect::Any)?;
    println!("after delete: {:?}", db.get(&key)?.map(|v| v.len()));
    Ok(())
}
