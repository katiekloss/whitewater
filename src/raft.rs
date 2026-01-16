use core::panic;
use std::{collections::HashMap, io::{self}, net::SocketAddr, sync::{RwLock}};

use tokio::sync::{broadcast::{self}, mpsc};
use whitewater::{CompleteLogEntry, IncomingRaftFrame, RaftFrame};
use crate::log::RaftLog;

#[derive(Clone)]
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

    pub async fn run(mut self, frame_queue: broadcast::Receiver<IncomingRaftFrame>) -> io::Result<()> {

        tokio::select! {
            r = self.handle_frames(frame_queue) => r,
        }
    }

    async fn handle_frames(&mut self, mut frame_queue: broadcast::Receiver<IncomingRaftFrame>) -> io::Result<()> {
        println!("Raft started");

        loop {
            match frame_queue.recv().await {
                Ok(IncomingRaftFrame::Connect(peer, queue)) => {
                    println!("{peer} connected");

                    let core = self.log.read().unwrap();
                    if let Err(e) = queue.send(RaftFrame::Initialize { current_position: core.commit_index }).await {
                        println!("Failed to initialize {peer}: {e}");
                    }

                    self.connections.insert(peer, queue);
                },
                Ok(IncomingRaftFrame::Normal { peer, frame}) => {
                    self.on_frame(peer, frame).await;
                },
                Ok(IncomingRaftFrame::Disconnect(peer)) => {
                    println!("{peer} disconnected");
                },
                Err(broadcast::error::RecvError::Closed) => {
                    return Ok(());
                },
                Err(e) => {
                    panic!("{e}");
                }
            }
        }
    }

    // this needs to be mut self in order for handle_set to take the lock,
    // but that moves self out of the main loop, which is what I'm getting stuck on every time
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

    async fn handle_set(&mut self, key: String, value: String) {
        let mut log = self.log.write().unwrap();

        let entry = log.write_log(key, value).expect("Failed to write log");

        if let Err(e) = self.ev_send.send(Event::WriteCommitted(entry)) {
            panic!("{e}");
        }
    }
}
