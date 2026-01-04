use std::{io::{Read, Write}, net::TcpStream};

use whitewater::{SetRequest, SetResponse};

fn main() {
    let mut socket = TcpStream::connect("127.0.0.1:51778").unwrap();
    let msg = rmp_serde::to_vec(&SetRequest {
        key: "hello".to_string(),
        value: "world".to_string()
    }).unwrap();

    socket.write(&msg).unwrap();

    let mut buf = vec![0; 8192];
    let n = socket.read(&mut buf).unwrap();
    let buf = &buf[..n];
    println!("{:?}", rmp_serde::from_slice::<SetResponse>(&buf));
}