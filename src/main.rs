use std::io;
use std::sync::Arc;
use tokio::sync::mpsc;
use crate::rpc::RpcServer;

mod kv;
use crate::kv::Kv;

mod raft;
use crate::raft::Raft;

mod rpc;

#[tokio::main]
async fn main() -> io::Result<()>{
    let (rpc_tx, rpc_rx) = mpsc::channel(16);

    let rpc = RpcServer {
        rpc_channel: rpc_tx
    };

    let raft = Arc::new(Raft::new());

    let kv = Kv {
        rpc_channel: rpc_rx,
        raft: raft.clone()
    };

    tokio::select! {
        r = rpc.listen_rpc() => {
            if let Err(e) = r {
                eprintln!("Listener aborted: {}", e);
            }
        },
        r = kv.run() => {
            if let Err(e) = r {
                eprintln!("KV store aborted: {}", e);
            }
        },
        r = raft.run() => {
            if let Err(e) = r {
                eprintln!("Raft aborted: {}", e);
            }
        }
    }

    Ok(())
}
