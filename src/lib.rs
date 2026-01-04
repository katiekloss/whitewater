use serde::{Deserialize, Serialize};

pub struct Raft {
    pub commit_index: i64,
    pub last_applied_index: i64,
    pub state: RaftState
}

pub enum RaftState {
    Leader,
    Follower,
    Candidate
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RpcRequest {
    pub message: String
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RpcResponse {
    pub message: String
}