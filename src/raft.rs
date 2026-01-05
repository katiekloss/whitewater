use std::{io, sync::{LazyLock, Mutex}};

use tokio::{fs::OpenOptions, io::AsyncWriteExt};
use whitewater::{RaftLogEntry, SetRequest};

pub async fn write_log(term: i64, write: &SetRequest) -> io::Result<()> {
    static LOG_MUTEX: LazyLock<Mutex<bool>> = LazyLock::new(|| Mutex::new(false));

    let _handle = LOG_MUTEX.lock();
    let mut log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open("raft.log")
        .await?;

    let buf = rmp_serde::to_vec(&RaftLogEntry {
        term,
        key: &write.key,
        value: &write.value
    }).unwrap();

    log_file.write_all(&buf).await?;

    Ok(())
}