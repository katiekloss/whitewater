use std::io;
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream};
use whitewater::RaftFrame;

#[tokio::main]
async fn main() -> io::Result<()> {
    let mut raft_socket = TcpStream::connect("127.0.0.1:7778").await?;
    raft_socket.write(&rmp_serde::to_vec(&RaftFrame::Heartbeat).unwrap()).await?;
    let (mut rx, tx) = raft_socket.split();
    loop {
        let mut buf = vec![0; 8192];
        match rx.read(&mut buf).await {
            Ok(0) => {
                break;
            },
            Ok(n) => {
                println!("{:?}", rmp_serde::from_slice::<RaftFrame>(&buf[..n]));
            },
            Err(e) => {
                panic!("{}", e);
            }
        }
    }

    Ok(())
}