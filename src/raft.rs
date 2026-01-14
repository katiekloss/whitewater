use core::panic;
use std::{collections::HashMap, io::{self}, net::SocketAddr, sync::Mutex};

use tokio::sync::broadcast::{self};
use whitewater::{CompleteLogEntry, IncomingRaftFrame, RaftFrame};
use crate::log::RaftLog;

#[derive(Clone)]
pub enum Event {
    WriteCommitted(CompleteLogEntry)
}

pub struct Raft {
    map: HashMap<String,String>,
    log: Mutex<RaftLog>,
    pub term: u64,
    pub commit_index: u64,
    pub global_send: Option<broadcast::Sender<Event>>
}

impl Raft {
    pub(crate) fn new() -> io::Result<Self> {
        let map = HashMap::new();

        Ok(Self {
            map,
            log: Mutex::new(RaftLog::new()),
            term: 0,
            commit_index: 0,
            global_send: None
        })
    }

    pub async fn run(mut self, frame_queue: broadcast::Receiver<IncomingRaftFrame>) -> io::Result<()> {
        {
            let mut log = self.log.lock().unwrap();
            (self.term, self.commit_index, self.map) = log.load().await;
        }

        tokio::select! {
            r = self.handle_frames(frame_queue) => r,
        }
    }

    async fn handle_frames(&self, mut frame_queue: broadcast::Receiver<IncomingRaftFrame>) -> io::Result<()> {
        println!("Raft started");

        // this keeps the queues from being dropped in the loop, which immediately hangs up on the peer,
        // but they eventually need to go somewhere so we can talk to our friends
        let mut queues = vec![];
        loop {
            match frame_queue.recv().await {
                Ok(IncomingRaftFrame::Connect(peer, queue)) => {
                    println!("{peer} connected");

                    if let Err(e) = queue.send(RaftFrame::Initialize { current_position: self.commit_index }).await {
                        println!("Failed to initialize {peer}: {e}");
                    }

                    queues.push(queue);
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
    async fn on_frame(&self, peer: SocketAddr, frame: RaftFrame) {
        println!("{peer}: {frame:?}");
    }

    async fn handle_set(&mut self, key: String, value: String) {
        let log = self.log.lock().unwrap();

        match log.write_log(key, value) {
            Ok(entry) => self.global_send.as_ref().expect("Raft was not properly initialized").send(Event::WriteCommitted(entry)),
            Err(e) => panic!("{e}")
        };
    }
}
