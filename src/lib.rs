use std::{fmt::Display, net::SocketAddr};

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum RaftMode {
    Leader,
    Follower
}

#[derive(Clone, Debug)]
pub enum IncomingRaftFrame {
    Mode(RaftMode),
    Connect(SocketAddr, mpsc::Sender<RaftFrame>),
    Normal {
        peer: SocketAddr,
        frame: RaftFrame
    },
    Disconnect(SocketAddr)
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum RaftFrame {
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
    },
    Ack {
        term: u64,
        index: u64
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

impl Into<ShortLogEntry> for CompleteLogEntry {
    fn into(self) -> ShortLogEntry {
        ShortLogEntry { key: self.key, value: self.value }
    }
}