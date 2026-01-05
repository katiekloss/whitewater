use std::io;
use tokio::sync::mpsc;
use crate::rpc::RpcServer;

mod kv;
use crate::kv::Kv;

mod raft;

mod rpc;

#[tokio::main]
async fn main() -> io::Result<()>{
    let (rpc_tx, rpc_rx) = mpsc::channel(16);

    let rpc = RpcServer {
        rpc_channel: rpc_tx
    };

    let kv = Kv {
        rpc_channel: rpc_rx
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
        }
    }

    Ok(())
}