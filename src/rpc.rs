use std::{io::{Error, ErrorKind}, net::SocketAddr};

use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream, tcp::WriteHalf}, sync::{broadcast, mpsc}};
use whitewater::{IncomingRaftFrame, RaftFrame};

pub struct RpcListener {
}

impl RpcListener {
    pub async fn run(&self, queue: broadcast::Sender<IncomingRaftFrame>) -> Result<(), Error> {
        queue.send(IncomingRaftFrame::Mode(whitewater::RaftMode::Leader));

        let listener = TcpListener::bind("0.0.0.0:7778").await?;
        println!("RPC started");

        loop {
            let (conn, addr) = listener.accept().await?;
            let its_queue = queue.clone();
            _ = tokio::spawn(async move {
                if let Err(e) = Self::handle_connection(its_queue, conn, addr).await {
                    println!("Connection with {addr} dropped: {e}");
                }
            });
        }
    }

    pub async fn join(&self, peer: SocketAddr, queue: broadcast::Sender<IncomingRaftFrame>) -> Result<(), Error> {
        queue.send(IncomingRaftFrame::Mode(whitewater::RaftMode::Follower));

        let socket = TcpStream::connect(peer).await?;
        println!("RPC started");
        Self::handle_connection(queue.clone(), socket, peer).await?;

        Ok(())
    }

    async fn handle_connection(queue: broadcast::Sender<IncomingRaftFrame>, mut conn: TcpStream, peer: SocketAddr) -> Result<(), Error> {
        println!("RPC connected to {peer:?}");

        let (mut conn_rx, mut conn_tx) = conn.split();
        let (send_queue_tx, mut send_queue_rx) = mpsc::channel(16);

        if let Err(e) = queue.send(IncomingRaftFrame::Connect(peer, send_queue_tx)) {
            eprintln!("Failed to start connection to {peer}: {e}");
            let _ = conn_tx.shutdown();
            return Err(ErrorKind::BrokenPipe.into());
        }

        loop {
            // make this less silly
            let mut buf = vec![0; 8192];

            let result = tokio::select! {
                r = send_queue_rx.recv() => {
                    Self::on_raft_dequeue(r, &mut conn_tx).await
                },
                r = conn_rx.read(&mut buf) => {
                    match r {
                        Ok(0) => Err(ErrorKind::ConnectionAborted.into()),
                        Ok(n) => Self::on_net_receive(&buf[..n], peer, &queue).await, // this clones peer btw
                        Err(e) => Err(e)
                    }
                }
            };

            if let Err(e) = result {
                return Err(e);
            }
        }
    }

    async fn on_raft_dequeue(read_result: Option<RaftFrame>, conn_tx: &mut WriteHalf<'_>) -> Result<(), Error> {
        match read_result {
            Some(frame) => {
                match rmp_serde::to_vec(&frame) {
                    Ok(buf) => {
                        if let Err(e) = conn_tx.write(&buf).await {
                            eprintln!("Failed to send dequeued frame: {e}");
                        }
                    },
                    Err(e) => {
                        panic!("Frame to send won't serialize: {e}");
                    }
                }
            },
            None => {
                println!("Hanging up on {}", conn_tx.peer_addr().unwrap());
                return Err(ErrorKind::ConnectionAborted.into());
            }
        }

        Ok(())
    }

    async fn on_net_receive(buf: &[u8], peer: SocketAddr, mut queue: &broadcast::Sender<IncomingRaftFrame>) -> Result<(), Error> {

        match rmp_serde::from_slice::<RaftFrame>(buf) {
            Ok(frame) => {
                if let Err(e) = queue.send(IncomingRaftFrame::Normal { peer, frame }) {
                    panic!("Can't queue received frame: {e}");
                }
            },
            Err(e) => {
                //eprintln!("{peer} sent nonsense: {e}");
            }
        }
        
        Ok(())
    }
}