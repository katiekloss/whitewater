use std::{io::{Error, ErrorKind, Read}, net::SocketAddr};

use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream, tcp::WriteHalf}, sync::mpsc};
use whitewater::{RaftFrame};

#[derive(Debug)]
pub enum RpcConnectionEvent {
    Connected(SocketAddr, mpsc::Receiver<RaftFrame>, mpsc::Sender<RaftFrame>)
}

pub struct RpcListener {
    // maybe just move this into run and join
    pub connection_queue: mpsc::Sender<RpcConnectionEvent>
}

impl RpcListener {
    pub async fn run(&self) -> Result<(), Error> {
        let listener = TcpListener::bind("0.0.0.0:7778").await?;
        loop {
            let (conn, addr) = listener.accept().await?;
            let its_queue = self.connection_queue.clone();
            _ = tokio::spawn(async move {
                _ = Self::handle_connection(its_queue, conn, addr).await;
            });
        }
    }

    pub async fn join(&self, peer: SocketAddr) -> Result<(), Error> {
        let socket = TcpStream::connect(peer).await?;
        Self::handle_connection(self.connection_queue.clone(), socket, peer).await?;

        Ok(())
    }

    async fn handle_connection(connection_queue: mpsc::Sender<RpcConnectionEvent>, mut conn: TcpStream, peer: SocketAddr) -> Result<(), Error> {
        println!("RPC connected to {peer:?}");

        let (mut conn_rx, mut conn_tx) = conn.split();
        let (recv_queue_tx, recv_queue_rx) = mpsc::channel(16);
        let (send_queue_tx, mut send_queue_rx) = mpsc::channel(16);

        if let Err(e) = connection_queue.send(RpcConnectionEvent::Connected(peer, recv_queue_rx, send_queue_tx)).await {
            eprintln!("Failed to start connection to {peer}: {e}");
            let _ = conn_tx.shutdown();
            return Err(ErrorKind::BrokenPipe.into());
        }

        loop {
            // make this less silly
            let mut buf = vec![0; 8192];

            let _ = tokio::select! {
                r = send_queue_rx.recv() => {
                    Self::on_raft_dequeue(r, &mut conn_tx).await;

                    // not necessarily
                    return Ok(())
                },
                r = conn_rx.read(&mut buf) => {
                    match r {
                        Ok(0) => {

                        },
                        Ok(n) => {
                            Self::on_net_receive(&buf[..n], &recv_queue_tx).await;
                        },
                        Err(e) => {

                        }
                    }

                    // this either
                    return Ok(())
                }
            };
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
                //println!("Hanging up on {peer}");
                return Ok(())
            }
        }

        Ok(())
    }

    async fn on_net_receive(buf: &[u8], mut recv_queue_tx: &mpsc::Sender<RaftFrame>) -> Result<(), Error> {

        match rmp_serde::from_slice::<RaftFrame>(buf) {
            Ok(frame) => {
                if let Err(e) = recv_queue_tx.send(frame).await {
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