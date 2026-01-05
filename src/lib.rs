use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

pub struct Raft {
    pub term: i64,
    pub commit_index: i64,
    pub last_applied_index: i64,
    pub state: RaftState
}

pub enum RaftState {
    Leader,
    Follower,
    Candidate
}

pub struct RpcLifecycle {
    pub request: SetRequest,
    pub sender: oneshot::Sender<SetResponse>
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SetRequest {
    pub key: String,
    pub value: String
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SetResponse {
    pub message: String
}

#[derive(Serialize)]
pub struct RaftLogEntry<'a> {
    pub term: i64,
    pub key: &'a String,
    pub value: &'a String
}