use std::net::SocketAddr;

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

#[derive(Clone, Debug)]
pub enum IncomingRaftFrame {
    Connect(SocketAddr, mpsc::Sender<RaftFrame>),
    Normal {
        peer: SocketAddr,
        frame: RaftFrame
    },
    Disconnect(SocketAddr)
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum RaftFrame {
    Initialize {
        current_position: u64
    },
    AppendLogs {
        term: u64,
        prev_log_index: u64,
        prev_log_term: u64,
        commit_index: u64,
        logs: Vec<ShortLogEntry>
    },
    Set {
        key: String,
        value: String
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ShortLogEntry {
    pub key: String,
    pub value: String
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CompleteLogEntry {
    pub term: u64,
    pub index: u64,
    pub key: String,
    pub value: String
}