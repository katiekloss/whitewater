use std::{io, net::SocketAddr};

use tokio::{io::AsyncReadExt, net::{TcpListener, TcpStream}};

#[tokio::main]
async fn main() -> io::Result<()>{
    tokio::select! {
        _ = listen() => { println!("Listener aborted"); }
    }

    Ok(())
}

async fn listen() -> io::Result<()> {
    let socket = TcpListener::bind("0.0.0.0:0").await?;
    println!("Listening on port {}", socket.local_addr().unwrap().port());

    loop {
        let (conn, addr) = socket.accept().await.unwrap();

        tokio::spawn(async move {
            handle_connection(conn, addr).await;
        });
    }
}

async fn handle_connection(mut conn: TcpStream, remote_addr: SocketAddr) {
    println!("Accepted connection from {}", remote_addr);
    let (mut rx, tx) = conn.split();

    loop {
        if let Err(e) = rx.readable().await {
            eprintln!("Connection from {} dropped: {}", remote_addr, e);
            return;
        }

        let mut buf = vec![0; 8192];
        match rx.read(&mut buf).await {
            Ok(0) => {
                eprintln!("Connection from {} dropped", remote_addr);
                return;
            },
            Ok(n) => {
                let buf = &buf[..n];
                println!("{:?}", buf);
            },
            Err(e) => {
                // technically shouldn't happen since readable() succeeded
                if e.kind() == io::ErrorKind::WouldBlock {
                    continue;
                }

                eprintln!("Failed to read from {}: {}", remote_addr, e);
                return;
            },

        }
    }
}
