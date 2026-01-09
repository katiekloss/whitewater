use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum RaftFrame {
    AppendLogs(AppendLogsFrame),
    Set(String, String)
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AppendLogsFrame {
    pub term: u64,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub commit_index: u64,
    pub logs: Vec<ShortLogEntry>
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ShortLogEntry {
    pub key: String,
    pub value: String
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CompleteLogEntry {
    pub term: i64,
    pub index: i64,
    pub key: String,
    pub value: String
}