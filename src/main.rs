use std::io;
use std::net::SocketAddr;
use clap::Parser;
use tokio::sync::mpsc;

mod raft;
use crate::log::RaftLog;
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

    let (connection_queue_tx, connection_queue_rx) = mpsc::channel(32);
    let (log_tx, log_rx) = mpsc::channel(32);

    let mut log = RaftLog::new(log_rx);
    log.load().await;

    let mut raft = Raft::new(connection_queue_rx, log_tx)?;

    // these very much don't belong here
    {
        let commit_index = log.commit_index.lock().unwrap();
        raft.term = log.term;
        raft.commit_index = *commit_index;
    }

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
        r = raft.run() => { println!("Raft aborted: {r:?}") },
        r = run_rpc() => { println!("RPC aborted: {r:?}") },
        r = log.run() => { println!("Log aborted: {r:?}") }
    }

    Ok(())
}
