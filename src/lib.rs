use serde::{Deserialize, Serialize};

// can this be done without Clone?
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum RaftFrame {
    AppendLogs(Vec<RaftLogEntry>),
    Set(String, String)
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RaftLogEntry {
    pub term: i64,
    pub index: i64,
    pub key: String,
    pub value: String
}