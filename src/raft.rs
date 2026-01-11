use core::panic;
use std::{collections::HashMap, fs::OpenOptions, io::{self, Error, Read, Write}, net::SocketAddr, sync::Mutex};

use tokio::sync::mpsc::{self};
use whitewater::{CompleteLogEntry, RaftFrame};

use crate::rpc::RpcConnectionEvent;


pub struct Raft {
    pub term: i64,
    connection_queue: mpsc::Receiver<RpcConnectionEvent>,
    /// Protects both the commit index and the log file
    commit_index: Mutex<i64>,
    //log_file: File,
    map: HashMap<String,String>,
    log: Vec<CompleteLogEntry>,
}

struct RaftConnection<'a> {
    addr: SocketAddr,
    send: &'a mpsc::Sender<RaftFrame>,
    receive: &'a mpsc::Receiver<RaftFrame>,
    next_index: u64,
    replicated_index: u64
}

impl Raft {
    pub(crate) async fn new(connection_queue: mpsc::Receiver<RpcConnectionEvent>) -> io::Result<Self> {
        let mut term = 0;
        let mut index = 0;
        let mut map = HashMap::new();
        let mut log = vec![];

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
                
                match rmp_serde::from_slice::<CompleteLogEntry>(&msgpack_buf) {
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
            connection_queue,
            term: term,
            commit_index: Mutex::new(index),
            map,
            log
        })
    }

    pub async fn run(self) -> io::Result<()> {
        let result;
        
        tokio::select! {
            r = self.leader() => result = r
        }

        // is there something like C#'s AggregateException?
        result
    }

    async fn leader(mut self) -> io::Result<()> {
        println!("Raft starting");
        let mut conns = vec![];

        loop {
            let conn = self.connection_queue.recv().await;
            match conn {
                Some(RpcConnectionEvent::Connected(peer, recv, send)) => {
                    conns.push(tokio::spawn(async move {
                        Self::handle(peer, send, recv).await;
                    }));
                },
                None => {
                    return Ok(())
                }
            }
        }
    }

    async fn handle(peer: SocketAddr, _send: mpsc::Sender<RaftFrame>, mut recv: mpsc::Receiver<RaftFrame>) -> Result<(), Error> {
        loop {
            match recv.recv().await {
                Some(frame) => {
                    println!("{peer}: {frame:?}");
                },
                None => {
                    break;
                }
            }
        }
        
        Ok(())
    }

    /// Writes a KV pair to disk, appends a log entry for it to the queue, and returns its commit index
    pub async fn write_log(&self, key: &String, value: &String) -> io::Result<i64> {
        let mut commit_index = self.commit_index.lock().unwrap();
        *commit_index += 1;

        let mut log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open("raft.log")?;

        let log = CompleteLogEntry {
            term: self.term,
            index: *commit_index,
            key: key.clone(), // TODO: zero copy somehow
            value: value.clone()
        };

        let buf = rmp_serde::to_vec(&log).unwrap();

        log_file.write_all(&buf)?;
        // let logs_sent = self.broadcaster.send(RaftFrame::AppendLogs(todo!()));

        // if let Err(e) = logs_sent {
        //     eprintln!("Failed to send logs: {e}");
        // }

        Ok(*commit_index)
    }
}
