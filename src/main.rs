use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use clap::Parser;

mod raft;
use crate::raft::Raft;

#[derive(Parser, Debug)]
struct Args {
    #[arg()]
    peer: Option<SocketAddr>,
}

#[tokio::main]
async fn main() -> io::Result<()>{
    let args = Args::parse();
    let raft = Arc::new(Raft::new());

    if let Some(peer) = args.peer {
        raft.join(peer).await
    } else {
        raft.run().await
    }
}
