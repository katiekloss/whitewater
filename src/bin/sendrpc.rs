use std::{io::Write, net::TcpStream};

use whitewater::RaftFrame;


fn main() {
    let mut socket = TcpStream::connect("127.0.0.1:7778").unwrap();
    socket.write(&rmp_serde::to_vec(&RaftFrame::Set{ key: "hello".to_string(), value: "world!".to_string() }).unwrap()).unwrap();
}