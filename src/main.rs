use std::io;
use std::net::SocketAddr;
use clap::Parser;
use tokio::sync::broadcast;

mod raft;
use crate::raft::Raft;
use crate::rpc::RpcListener;

mod rpc;

mod log;

#[derive(Parser, Debug)]
struct Args {
    #[arg()]
    peer: Option<SocketAddr>,
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let args = Args::parse();

    let (rpc_queue_tx, rpc_queue_rx) = broadcast::channel(128);

    let raft = Raft::new().await?;

    let rpc = RpcListener {};

    let run_rpc = async || {
        if let Some(peer) = args.peer {
            rpc.join(peer, rpc_queue_tx).await
        } else {
            rpc.run(rpc_queue_tx).await
        }
    };

    tokio::select! {
        r = raft.run(rpc_queue_rx) => { println!("Raft aborted: {r:?}") },
        r = run_rpc() => { println!("RPC aborted: {r:?}") }
    }

    Ok(())
}
