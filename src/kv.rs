use std::{collections::HashMap, io};

use tokio::sync::mpsc::Receiver;
use whitewater::{Raft, RpcLifecycle, SetResponse};
use crate::raft;

pub struct Kv {
    pub rpc_channel: Receiver<RpcLifecycle>
}

impl Kv {
    pub async fn run(mut self) -> io::Result<()> {
        let mut map = HashMap::new();
        // move this elsewhere
        let raft = Raft {
            term: 0,
            commit_index: 0,
            last_applied_index: 0,
            state: whitewater::RaftState::Leader
        };

        loop {
            let req = self.rpc_channel.recv().await;
            match req {
                Some(r) => {
                    println!("{:?}", r.request);
                    raft::write_log(raft.term, &r.request).await?;
                    map.insert(r.request.key, r.request.value);
                    let _ = r.sender.send(SetResponse {
                        message: "set".to_string()
                    });
                },
                None => return Ok(())
            }
        }
    }
}