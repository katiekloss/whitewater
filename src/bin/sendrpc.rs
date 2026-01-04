use std::{io::{Read, Write}, net::TcpStream};
use whitewater::{RpcRequest, RpcResponse};

fn main() {
    let mut socket = TcpStream::connect("127.0.0.1:50290").unwrap();
    let msg = rmp_serde::to_vec(&RpcRequest {
        message: "hello world!".to_string()
    }).unwrap();

    socket.write(&msg).unwrap();

    let mut buf = vec![0; 8192];
    let n = socket.read(&mut buf).unwrap();
    let buf = &buf[..n];
    println!("{:?}", rmp_serde::from_slice::<RpcResponse>(&buf));
}