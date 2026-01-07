use std::{io::{self}, net::SocketAddr, sync::{Arc, LazyLock, Mutex}};

use tokio::{fs::OpenOptions, io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}, sync::{broadcast::{self, Sender, error::RecvError}, mpsc::{self}}};
use whitewater::{RaftFrame, RaftLogEntry, SetRequest};

pub enum RaftState {
    Leader,
    Follower,
    Candidate
}

pub struct Raft {
    pub term: i64,
    pub commit_index: i64,
    pub last_applied_index: i64,
    pub state: RaftState
}

impl Raft {
    pub(crate) fn new() -> Self {
        Self { state: RaftState::Leader, term: 0, commit_index: 0, last_applied_index: 0 }
    }

    pub async fn run(self: Arc<Self>) -> io::Result<()> {
        let (tx, _) = tokio::sync::broadcast::channel(16);
        let (q_tx, q_rx) = tokio::sync::mpsc::channel(16);
        let result;

        tokio::select! {
            r = Self::run_listener(tx.clone(), q_tx) => result = r,
            r = Self::run_raft(q_rx, tx) => result = r
        }

        // is there something like C#'s AggregateException?
        result
    }

    pub async fn join(self, other_peer: SocketAddr) -> io::Result<()> {
        let conn = TcpStream::connect(other_peer).await?;

        Ok(())
    }

    async fn run_listener(broadcaster: Sender<RaftFrame>, raft_queue: mpsc::Sender<RaftFrame>) -> io::Result<()> {
        let listener = TcpListener::bind("0.0.0.0:7778").await?;

        loop {
            let (conn, addr) = listener.accept().await?;
            let receiver = broadcaster.subscribe();
            let its_queue = raft_queue.clone();
            tokio::spawn(async move {
                // handle this
                let _ = Self::handle_connection(conn, addr, receiver, its_queue).await;
            });
        }
    }

    async fn run_raft(mut queue: mpsc::Receiver<RaftFrame>, broadcaster: broadcast::Sender<RaftFrame>) -> io::Result<()> {
        loop {
            if let Some(frame) = queue.recv().await {
                println!("Got a {:?}", frame);
                let test = RaftLogEntry {
                    key: "hello".to_string(),
                    term: 42,
                    value: "world".to_string()
                };
                
                if broadcaster.send(RaftFrame::AppendLogs(vec![test])).is_err() {
                    eprintln!("Failed to send back AppendLogs");
                }
            } else {
                return Ok(())
            }
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
                        match rmp_serde::to_vec(&frame) {
                            Ok(buf) => {
                                tx.write(&buf).await;
                                Ok(())
                            },
                            Err(e) => panic!("Serialization error: {}", e)
                        }
                    },
                    Err(RecvError::Closed) => Err("not actually an error we're just done".to_string()),
                    Err(RecvError::Lagged(_)) => panic!("Dropped frames")
                }
            };
        }
    }

    pub async fn write_log(&self, write: &SetRequest) -> io::Result<()> {
        static LOG_MUTEX: LazyLock<Mutex<bool>> = LazyLock::new(|| Mutex::new(false));

        let _handle = LOG_MUTEX.lock().unwrap();
        let mut log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open("raft.log")
            .await?;

        let buf = rmp_serde::to_vec(&RaftLogEntry {
            term: self.term,
            key: write.key.clone(), // TODO: zero copy somehow
            value: write.value.clone()
        }).unwrap();

        log_file.write_all(&buf).await?;

        Ok(())
    }
}
