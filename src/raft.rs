use core::panic;
use std::{collections::HashMap, fs::OpenOptions, io::{self, Read, Write}, net::SocketAddr, sync::Mutex, time::Duration};

use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}, sync::{broadcast::{self, Sender, error::RecvError}, mpsc::{self}}};
use whitewater::{RaftFrame, RaftLogEntry};

pub struct Raft {
    pub term: i64,
    broadcaster: Sender<RaftFrame>,
    /// Protects both the commit index and the log file
    commit_index: Mutex<i64>,
    //log_file: File,
    map: HashMap<String,String>,
    log: Vec<RaftLogEntry>
}

impl Raft {
    pub(crate) async fn try_load() -> io::Result<Self> {
        let mut term = 0;
        let mut index = 0;
        let mut map = HashMap::new();
        let mut log = vec![];
        let (broadcaster, _) = broadcast::channel(32);

        let log_open = OpenOptions::new()
            .read(true)
            .open("raft.log");

        if log_open.is_ok() {
            let mut log_file = log_open.unwrap();
            let mut msgpack_buf = vec![];

            // read one byte at a time (don't @ me) and attempt to deserialize what we have so far into a log entry.
            // when we read a complete entry, add it to the vector and try to do it again, until we reach the end of the file.
            loop {
                let mut buf = vec![0; 1];
                match log_file.read(&mut buf) {
                    Ok(0) => break,
                    Ok(_) => msgpack_buf.append(&mut buf),
                    Err(e) => panic!("{e}")
                };
                
                match rmp_serde::from_slice::<RaftLogEntry>(&msgpack_buf) {
                    Ok(entry) => {
                        msgpack_buf.clear();
                        map.insert(entry.key.clone(), entry.value.clone());
                        if term < entry.term {
                            term = entry.term
                        }

                        if index < entry.index {
                            index = entry.index;
                        }
                        log.push(entry);
                    }
                    _ => {}
                }
            }

            println!("Loaded {} entries, term {term}, index {index}", log.len());
        }

        Ok(Self {
            broadcaster,
            term: term,
            commit_index: Mutex::new(index),
            map,
            log
        })
    }

    pub async fn run(self: Box<Self>) -> io::Result<()> {
        let (raft_queue_tx, raft_queue_rx) = tokio::sync::mpsc::channel(16);

        let result;
        
        tokio::select! {
            r = Self::heartbeat(self.broadcaster.clone()) => result = r,
            r = self.leader(raft_queue_rx) => result = r
        }

        // is there something like C#'s AggregateException?
        result
    }

    pub async fn join(self: Box<Self>, other_peer: SocketAddr) -> io::Result<()> {
        let conn = TcpStream::connect(other_peer).await?;
        let (raft_queue_tx, raft_queue_rx) = tokio::sync::mpsc::channel(16);

        self.follower(raft_queue_rx).await
    }

    async fn leader(mut self: Box<Self>, mut queue: mpsc::Receiver<RaftFrame>) -> io::Result<()> {
        println!("Leader starting");

        loop {
            let frame = queue.recv().await;
            if frame.is_none() {
                // channel closed
                return Ok(())
            }
            let frame = frame.unwrap();
            
            println!("Got a {frame:?}");
            
            match frame {
                RaftFrame::Set(key, value) => {
                    self.write_log(&key, &value).await;
                },
                RaftFrame::AppendLogs(_) => {
                    panic!("Another node sent me logs but that's my job");
                }
            }
        }
    }

    async fn follower(self: Box<Self>, mut raft_queue: mpsc::Receiver<RaftFrame>) -> io::Result<()> {
        println!("Follower starting");
        loop {
            println!("Got {:?}", raft_queue.recv().await);
        }
    }

    /// Occasionally sends an AppendLogs RPC with zero entries so that followers know we're still the leader
    async fn heartbeat(broadcaster: broadcast::Sender<RaftFrame>) -> io::Result<()> {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let _ = broadcaster.send(RaftFrame::AppendLogs(vec![]));
        }
    }

    /// Writes a KV pair to disk, appends a log entry for it to the queue, and returns its commit index
    pub async fn write_log(self: &Box<Self>, key: &String, value: &String) -> io::Result<i64> {
        let mut commit_index = self.commit_index.lock().unwrap();
        *commit_index += 1;

        let mut log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open("raft.log")?;

        let log = RaftLogEntry {
            term: self.term,
            index: *commit_index,
            key: key.clone(), // TODO: zero copy somehow
            value: value.clone()
        };

        let buf = rmp_serde::to_vec(&log).unwrap();

        log_file.write_all(&buf)?;
        let logs_sent = self.broadcaster.send(RaftFrame::AppendLogs(vec![log]));

        if let Err(e) = logs_sent {
            eprintln!("Failed to send logs: {e}");
        }

        Ok(*commit_index)
    }
}
