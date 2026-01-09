use core::panic;
use std::{collections::HashMap, fs::OpenOptions, io::{self, Read, Write}, net::SocketAddr, sync::{Mutex}};

use tokio::{net::TcpStream, sync::mpsc::self};
use whitewater::{CompleteLogEntry, RaftFrame};

use crate::rpc::RpcConnectionEvent;


pub struct Raft {
    pub term: i64,
    connection_queue: mpsc::Receiver<RpcConnectionEvent>,
    /// Protects both the commit index and the log file
    commit_index: Mutex<i64>,
    //log_file: File,
    map: HashMap<String,String>,
    log: Vec<CompleteLogEntry>
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

    pub async fn run(mut self) -> io::Result<()> {
        let result;
        
        tokio::select! {
            r = self.leader() => result = r
        }

        // is there something like C#'s AggregateException?
        result
    }

    async fn leader(&mut self) -> io::Result<()> {
        println!("Leader starting");

        loop {
            let conn = self.connection_queue.recv().await;
            match conn {
                Some(RpcConnectionEvent::Connected(peer, recv, send)) => {
                    println!("{peer} connected");
                },
                None => {
                    return Ok(())
                }
            }
        }
    }

    async fn follower(&self, mut raft_queue: mpsc::Receiver<RaftFrame>) -> io::Result<()> {
        println!("Follower starting");
        loop {
            println!("Got {:?}", raft_queue.recv().await);
        }
    }

    /// Occasionally sends an AppendLogs RPC with zero entries so that followers know we're still the leader
    // async fn heartbeat(broadcaster: broadcast::Sender<RaftFrame>) -> io::Result<()> {
    //     loop {
    //         tokio::time::sleep(Duration::from_secs(1)).await;

    //         let send = broadcaster.send(RaftFrame::AppendLogs(AppendLogsFrame{
    //             term: 0,
    //             prev_log_index: 0,
    //             prev_log_term: 0,
    //             commit_index: 0,
    //             logs: vec![]
    //         }));

    //         if let Err(e) = send {
    //             panic!("idk: {e}");
    //         }
    //     }
    // }

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
