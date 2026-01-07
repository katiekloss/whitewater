use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use clap::Parser;
use tokio::sync::mpsc;
use crate::rpc::RpcServer;

mod kv;
use crate::kv::Kv;

mod raft;
use crate::raft::Raft;

mod rpc;

#[derive(Parser, Debug)]
struct Args {
    #[arg()]
    peer: Option<SocketAddr>,
}

#[tokio::main]
async fn main() -> io::Result<()>{
    let args = Args::parse();

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
        r = raft_start(raft, args) => {
            if let Err(e) = r {
                eprintln!("Raft aborted: {}", e);
            }
        }
    }

    Ok(())
}

async fn raft_start(raft: Arc<Raft>, args: Args) -> io::Result<()> {
    if let Some(peer) = args.peer {
        raft.join(peer).await
    } else {
        raft.run().await
    }
}
