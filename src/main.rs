use std::io;
use std::net::SocketAddr;
use clap::Parser;
use tokio::sync::mpsc;
use tonic::transport::Server;

mod raft;
use crate::raft::Raft;
use crate::rpc::RaftRpc;
use crate::rpc::rpc::whitewater_rpc_server::WhitewaterRpcServer;

mod rpc;

#[derive(Parser, Debug)]
struct Args {
    #[arg()]
    peer: Option<SocketAddr>,
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let args = Args::parse();

    let (raft_queue_tx, raft_queue_rx) = mpsc::channel(16);

    let rpc_server = Server::builder()
        .add_service(WhitewaterRpcServer::new(RaftRpc::new(raft_queue_tx)));

    let raft = Box::new(Raft::try_load().await?);

    let run_raft = async || {
        if let Some(peer) = args.peer {
            raft.join(peer).await
        } else {
            raft.run().await
        }
    };

    tokio::select! {
        _ = run_raft() => {},
        _ = rpc_server.serve("0.0.0.0:7778".parse().unwrap()) => {}
    }

    Ok(())
}
