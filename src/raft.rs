use core::panic;
use std::{collections::HashMap, io::{self, ErrorKind}, net::SocketAddr, sync::RwLock};

use tokio::sync::{broadcast::{self}, mpsc};
use whitewater::{CompleteLogEntry, IncomingRaftFrame, RaftFrame};
use crate::log::RaftLog;

#[derive(Clone, Debug)]
pub enum Event {
    WriteCommitted(CompleteLogEntry)
}

pub struct Raft {
    log: RwLock<RaftLog>,
    connections: HashMap<SocketAddr, mpsc::Sender<RaftFrame>>,
    ev_send: broadcast::Sender<Event>,
    ev_recv: broadcast::Receiver<Event>
}

impl Raft {
    pub(crate) async fn new() -> io::Result<Self> {
        let (ev_send, ev_recv) = broadcast::channel(128);

        let log = RwLock::new(RaftLog::new());

        Ok(Self {
            connections: HashMap::new(),
            ev_send,
            ev_recv,
            log
        })
    }

    pub async fn run(mut self, mut frame_queue: broadcast::Receiver<IncomingRaftFrame>) -> io::Result<()> {
        loop {
            let result = tokio::select! {
                Ok(frame) = frame_queue.recv() => self.handle_frame(frame).await,
                Ok(event) = self.ev_recv.recv() => self.on_event(event).await,
                else => Err(ErrorKind::ConnectionAborted.into())
            };

            if let Err(e) = result {
                break;
            }
        }

        Ok(())
    }

    async fn handle_frame(&mut self, frame: IncomingRaftFrame) -> io::Result<()> {
        match frame {
            IncomingRaftFrame::Connect(peer, queue) => {
                println!("{peer} connected");

                let log = self.log.read().unwrap();
                if let Err(e) = queue.send(RaftFrame::Initialize { current_position: log.commit_index }).await {
                    println!("Failed to initialize {peer}: {e}");
                }

                self.connections.insert(peer, queue);
                return Ok(())
            },
            IncomingRaftFrame::Normal { peer, frame} => {
                self.on_frame(peer, frame).await;
                return Ok(())
            },
            IncomingRaftFrame::Disconnect(peer) => {
                println!("{peer} disconnected");
                return Ok(())
            }
        }
    }

    async fn on_frame(&mut self, peer: SocketAddr, frame: RaftFrame) {
        println!("{peer}: {frame:?}");
        match frame {
            RaftFrame::Set { key, value } => {
                self.handle_set(key, value).await;
            },
            _ => {

            }
        }
    }

    async fn on_event(&mut self, result: Event) -> io::Result<()> {
        println!("{result:?}");
        Ok(())
    }

    async fn handle_set(&mut self, key: String, value: String) {
        let mut log = self.log.write().unwrap();

        let entry = log.write_log(key, value).expect("Failed to write log");

        if let Err(e) = self.ev_send.send(Event::WriteCommitted(entry)) {
            panic!("{e}");
        }
    }
}
