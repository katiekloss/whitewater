use std::io;
use std::net::SocketAddr;
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
    let raft = Box::new(Raft::try_load().await?);

    if let Some(peer) = args.peer {
        raft.join(peer).await
    } else {
        raft.run().await
    }
}
