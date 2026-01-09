use std::io;
use std::net::SocketAddr;
use clap::Parser;
use tokio::sync::mpsc;

mod raft;
use crate::raft::Raft;
use crate::rpc::RpcListener;

mod rpc;

#[derive(Parser, Debug)]
struct Args {
    #[arg()]
    peer: Option<SocketAddr>,
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let args = Args::parse();

    let (connection_queue_tx, connection_queue_rx) = mpsc::channel(16);

    let raft = Raft::new(connection_queue_rx).await?;

    let rpc = RpcListener {
        connection_queue: connection_queue_tx
    };

    let run_rpc = async || {
        if let Some(peer) = args.peer {
            rpc.join(peer).await
        } else {
            rpc.run().await
        }
    };

    tokio::select! {
        _ = raft.run() => {},
        _ = run_rpc() => {}
    }

    Ok(())
}
