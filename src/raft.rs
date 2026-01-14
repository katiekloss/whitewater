use core::panic;
use std::{collections::HashMap, io::{self, Error}, net::SocketAddr};

use tokio::{sync::{mpsc::{self}, oneshot}, task::JoinSet};
use whitewater::{CompleteLogEntry, RaftFrame, ShortLogEntry};

use crate::{log::{LogRpc, LogWrite}, rpc::RpcConnectionEvent};

enum Event {
    WriteCommitted(CompleteLogEntry)
}

pub struct Raft {
    connection_queue: mpsc::Receiver<RpcConnectionEvent>,
    map: HashMap<String,String>,
    log: mpsc::Sender<LogRpc>,
    pub term: u64,
    pub commit_index: u64
}

struct RaftConnection {
    initialized: bool,
    addr: SocketAddr,
    recv: mpsc::Receiver<RaftFrame>,
    next_index: u64,
    current_position: u64,
    log: mpsc::Sender<LogRpc>
}

impl RaftConnection {
    async fn handle(&mut self, mut raft: mpsc::Sender<Event>) -> Result<(), Error> {
        loop {
            let frame = match self.recv.recv().await {
                Some(frame) => {
                    println!("{}: {:?}", self.addr, frame);
                    frame
                },
                None => {
                    break;
                }
            };

            match frame {
                RaftFrame::Initialize { current_position } => {
                    println!("{} is at {current_position}", self.addr);
                    self.current_position = current_position;
                    self.initialized = true;
                },
                RaftFrame::Set(key, value) => {
                    let (response_tx, response_rx) = oneshot::channel();
                    let write = LogRpc::Write {
                        entry: ShortLogEntry {
                            key,
                            value
                        },
                        response: response_tx
                    };

                    if let Err(e) = self.log.send(write).await {
                        panic!("{e}");
                    }

                    match response_rx.await {
                        Ok(entry) => {
                            if let Err(e) = raft.send(Event::WriteCommitted(entry)).await {
                                panic!("{e}");
                            }
                        },
                        Err(e) => {
                            panic!("{e}");
                        }
                    }
                },
                _ => {

                }
            }
        }
        
        Ok(())
    }
}

impl Raft {
    pub(crate) fn new(connection_queue: mpsc::Receiver<RpcConnectionEvent>, log: mpsc::Sender<LogRpc>) -> io::Result<Self> {
        let map = HashMap::new();

        Ok(Self {
            connection_queue,
            map,
            log,
            term: 0,
            commit_index: 0
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

    async fn leader(self) -> io::Result<()> {
        println!("Raft starting");
        let mut conns = JoinSet::new();

        let mut queue = self.connection_queue;
        let (self_tx, _self_rx) = mpsc::channel(32);

        loop {
            let conn_event = queue.recv().await;
            match conn_event {
                Some(RpcConnectionEvent::Connected(peer, recv, send)) => {
                    let mut conn = RaftConnection {
                        initialized: false,
                        addr: peer,
                        recv,
                        next_index: 0,
                        current_position: 0,
                        log: self.log.clone()
                    };

                    if let Err(e) = send.send(RaftFrame::Initialize { current_position: self.commit_index }).await {
                        panic!("{e}");
                    }

                    let self_tx = self_tx.clone();

                    conns.spawn(async move {
                        let _ = conn.handle(self_tx).await;
                    });
                },
                None => {
                    break;
                }
            }
        }

        Ok(())
    }
}
