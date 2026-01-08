use std::{collections::HashMap, io::{self}, net::SocketAddr, sync::{Arc, LazyLock, Mutex}, time::Duration};

use tokio::{fs::OpenOptions, io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}, sync::{broadcast::{self, Sender, error::RecvError}, mpsc::{self}}};
use whitewater::{RaftFrame, RaftLogEntry};

pub enum RaftState {
    Leader,
    Follower,
    Candidate
}

pub struct Raft {
    pub term: i64,
    pub commit_index: i64,
    pub last_applied_index: i64,
    pub state: RaftState,
    broadcaster: Sender<RaftFrame>
}

impl Raft {
    pub(crate) fn new() -> Self {
        let (broadcaster, _) = tokio::sync::broadcast::channel(16);
        Self { state: RaftState::Leader, term: 0, commit_index: 0, last_applied_index: 0, broadcaster }
    }

    pub async fn run(self: Arc<Self>) -> io::Result<()> {
        let (raft_queue_tx, raft_queue_rx) = tokio::sync::mpsc::channel(16);

        let result;
        
        tokio::select! {
            r = Self::run_listener(self.broadcaster.clone(), raft_queue_tx) => result = r,
            r = Self::heartbeat(self.broadcaster.clone()) => result = r,
            r = self.leader(raft_queue_rx) => result = r
        }

        // is there something like C#'s AggregateException?
        result
    }

    pub async fn join(self: Arc<Self>, other_peer: SocketAddr) -> io::Result<()> {
        let conn = TcpStream::connect(other_peer).await?;
        let (raft_queue_tx, raft_queue_rx) = tokio::sync::mpsc::channel(16);

        let result;

        tokio::select! {
            r = Self::handle_connection(conn, other_peer, self.broadcaster.subscribe(), raft_queue_tx) => result = r,
            r = self.follower(raft_queue_rx) => result = r
        }

        result
    }

    async fn run_listener(broadcaster: Sender<RaftFrame>, raft_queue: mpsc::Sender<RaftFrame>) -> io::Result<()> {
        let listener = TcpListener::bind("0.0.0.0:7778").await?;
        
        loop {
            let (conn, addr) = listener.accept().await?;
            let receiver = broadcaster.subscribe();
            let its_queue = raft_queue.clone();
            tokio::spawn(async move {
                println!("Connected to {}", addr);
                // handle this
                let _ = Self::handle_connection(conn, addr, receiver, its_queue).await;
            });
        }
    }

    async fn leader(self: Arc<Self>, mut queue: mpsc::Receiver<RaftFrame>) -> io::Result<()> {
        let mut map = HashMap::new();
        println!("Leader starting");

        loop {
            let frame = queue.recv().await;
            if frame.is_none() {
                // channel closed
                return Ok(())
            }
            let frame = frame.unwrap();
            
            println!("Got a {:?}", frame);
            
            match frame {
                RaftFrame::Set(key, value) => {
                    self.write_log(&key, &value).await;
                    map.insert(key, value);
                },
                RaftFrame::AppendLogs(_) => {
                    panic!("Another node sent me logs but that's my job");
                }
            }
        }
    }

    async fn follower(self: Arc<Self>, mut raft_queue: mpsc::Receiver<RaftFrame>) -> io::Result<()> {
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

    /// awaits two possible send paths, incoming Raft frames sent by the peer, and outgoing Raft frames intended for broadcast to all peers.
    /// Depending on which one produces data first, either receives the peer's frame into the global Raft queue or sends our frame to the peer, respectively.
    async fn handle_connection(mut conn: TcpStream, peer_addr: SocketAddr, mut broadcaster: broadcast::Receiver<RaftFrame>, raft_queue: mpsc::Sender<RaftFrame>) -> io::Result<()> {
        let (mut rx, mut tx) = conn.split();

        loop {
            let mut buf = vec![0; 8192];

            let result = tokio::select! {
                read_result = rx.read(&mut buf) => match read_result {
                    Ok(0) => Err(format!("Connection from {} dropped", peer_addr)),
                    Ok(n) => {
                        let buf = &buf[..n];
                        match rmp_serde::from_slice::<RaftFrame>(&buf) {
                            Ok(f) => {
                                if let Err(e) = raft_queue.send(f).await {
                                    println!("Failed to queue: {:?}", e)
                                }
                                Ok(())
                            },
                            Err(e) => Err(e.to_string())
                        }
                    },
                    Err(e) => Err(format!("Failed to read from {}: {}", peer_addr, e)),
                },
                f = broadcaster.recv() => match f {
                    Ok(frame) => {
                        println!("Sending {:?} to {}", frame, peer_addr);
                        match rmp_serde::to_vec(&frame) {
                            Ok(buf) => {
                                // this is probably bad Rust
                                if let Err(e) = tx.write(&buf).await {
                                    Err(e.to_string())
                                } else {
                                    Ok(())
                                }
                            },
                            Err(e) => panic!("Serialization error: {}", e)
                        }
                    },
                    Err(RecvError::Closed) => Err("not actually an error we're just done".to_string()),
                    Err(RecvError::Lagged(_)) => panic!("Dropped frames")
                }
            };

            if result.is_err() {
                return Ok(())
            }
        }
    }

    pub async fn write_log(self: &Arc<Self>, key: &String, value: &String) -> io::Result<()> {
        static LOG_MUTEX: LazyLock<Mutex<bool>> = LazyLock::new(|| Mutex::new(false));

        let _handle = LOG_MUTEX.lock().unwrap();
        let mut log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open("raft.log")
            .await?;

        let buf = rmp_serde::to_vec(&RaftLogEntry {
            term: self.term,
            key: key.clone(), // TODO: zero copy somehow
            value: value.clone()
        }).unwrap();

        log_file.write_all(&buf).await?;
        let logs_sent = self.broadcaster.send(RaftFrame::AppendLogs(vec![RaftLogEntry {
            term: self.term,
            key: key.clone(),
            value: value.clone()
        }]));

        if let Err(e) = logs_sent {
            eprintln!("Failed to send logs: {}", e);
        }

        Ok(())
    }
}
