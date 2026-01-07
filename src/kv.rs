use std::{collections::HashMap, io, sync::Arc};

use tokio::sync::mpsc::Receiver;
use whitewater::{RpcLifecycle, SetResponse};
use crate::raft::Raft;

pub struct Kv {
    pub rpc_channel: Receiver<RpcLifecycle>,
    pub raft: Arc<Raft>
}

impl Kv {
    pub async fn run(mut self) -> io::Result<()> {
        let mut map = HashMap::new();

        loop {
            let req = self.rpc_channel.recv().await;
            match req {
                Some(r) => {
                    println!("{:?}", r.request);
                    self.raft.write_log(&r.request).await?;
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