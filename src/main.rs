use std::{collections::HashMap, fmt::format, io, net::SocketAddr};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}, sync::mpsc::{self, Receiver, Sender}};
use whitewater::{Raft, RpcLifecycle, SetRequest, SetResponse};

#[tokio::main]
async fn main() -> io::Result<()>{
    let (rpc_tx, rpc_rx) = mpsc::channel(16);

    tokio::select! {
        _ = listen_rpc(rpc_tx) => { println!("Listener aborted"); },
        _ = state_machine(rpc_rx) => { println!("State machine aborted"); }
    }

    Ok(())
}

async fn listen_rpc(chan: Sender<RpcLifecycle>) -> io::Result<()> {
    let socket = TcpListener::bind("0.0.0.0:0").await?;
    println!("Listening on port {}", socket.local_addr().unwrap().port());

    loop {
        let (conn, addr) = socket.accept().await.unwrap();

        let chan_inner = chan.clone();
        tokio::spawn(async move {
            handle_connection(conn, addr, chan_inner).await;
        });
    }
}

async fn handle_connection(mut conn: TcpStream, remote_addr: SocketAddr, request_chan: Sender<RpcLifecycle>) {
    println!("Accepted connection from {}", remote_addr);
    let (mut conn_rx, mut conn_tx) = conn.split();

    loop {
        if let Err(e) = conn_rx.readable().await {
            eprintln!("Connection from {} dropped: {}", remote_addr, e);
            return;
        }

        let mut buf = vec![0; 8192];
        let request;
        match conn_rx.read(&mut buf).await {
            Ok(0) => {
                eprintln!("Connection from {} dropped", remote_addr);
                return;
            },
            Ok(n) => {
                let buf = &buf[..n];
                match rmp_serde::from_slice::<SetRequest>(&buf) {
                    Ok(r) => request = r,
                    Err(_) => {
                        eprintln!("Got garbage from {}", remote_addr);
                        return;
                    }
                }
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

        let (future_tx, future_rx) = tokio::sync::oneshot::channel();
        
        if let Err(e) = request_chan.send(RpcLifecycle { request, sender: future_tx }).await {
            eprintln!("Failed to queue request: {}", e);
        }

        let response = future_rx.await.unwrap();

        let buf = rmp_serde::to_vec(&response).unwrap();
        if let Err(e) = conn_tx.write(&buf).await {
            eprintln!("Failed to write response: {}", e);
        }
    }
}

async fn state_machine(mut incoming: Receiver<RpcLifecycle>) -> io::Result<()> {
    let mut map = HashMap::new();

    loop {
        let req = incoming.recv().await;
        match req {
            Some(r) => {
                println!("{:?}", r.request);
                map.insert(r.request.key, r.request.value);

                let _ = r.sender.send(SetResponse {
                    message: "set".to_string()
                });
            },
            None => return Ok(())
        }
    }
}
