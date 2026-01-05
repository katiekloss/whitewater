use std::{io, sync::{LazyLock, Mutex}};

use serde::Serialize;
use tokio::{fs::OpenOptions, io::AsyncWriteExt};
use whitewater::SetRequest;

pub enum RaftState {
    Leader,
    Follower,
    Candidate
}

#[derive(Serialize)]
pub struct RaftLogEntry<'a> {
    pub term: i64,
    pub key: &'a String,
    pub value: &'a String
}

pub struct Raft {
    pub term: i64,
    pub commit_index: i64,
    pub last_applied_index: i64,
    pub state: RaftState
}

impl Raft {
    pub(crate) fn new() -> Self {
        Self { state: RaftState::Leader, term: 0, commit_index: 0, last_applied_index: 0 }
    }

    pub async fn write_log(self, write: &SetRequest) -> io::Result<()> {
        static LOG_MUTEX: LazyLock<Mutex<bool>> = LazyLock::new(|| Mutex::new(false));

        let _handle = LOG_MUTEX.lock();
        let mut log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open("raft.log")
            .await?;

        let buf = rmp_serde::to_vec(&RaftLogEntry {
            term: self.term,
            key: &write.key,
            value: &write.value
        }).unwrap();

        log_file.write_all(&buf).await?;

        Ok(())
    }
}
