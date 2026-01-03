use std::{io, net::SocketAddr};

use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() -> io::Result<()>{
    let socket = TcpListener::bind("0.0.0.0:0").await?;
    println!("Listening on port {}", socket.local_addr().unwrap().port());

    loop {
        let (conn, addr) = socket.accept().await.unwrap();

        tokio::spawn(async move {
            handle_connection(conn, addr).await;
        });
    }
}

async fn handle_connection(conn: TcpStream, remote_addr: SocketAddr) {
    println!("Accepted connection from {}", remote_addr);
}