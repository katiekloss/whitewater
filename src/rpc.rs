use std::pin::Pin;

use tokio::sync::mpsc;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};
use whitewater::RaftRequest;

use crate::rpc::rpc::{SetReply, SetRequest, RaftMessage, whitewater_rpc_server::WhitewaterRpc};

pub mod rpc {
    tonic::include_proto!("whitewater"); // The string specified here must match the proto package name
}

#[derive(Debug)]
pub struct RaftRpc {
    queue: mpsc::Sender<RaftRequest>
}

impl RaftRpc {
    pub(crate) fn new(queue: mpsc::Sender<RaftRequest>) -> Self {
        Self { queue }
    }
}

#[tonic::async_trait]
impl WhitewaterRpc for RaftRpc {
    async fn set(&self, request: Request<SetRequest>) -> Result<Response<SetReply>, Status> {
        unimplemented!();
    }

    // TODO: understand wtf this means
    type RunStream = Pin<Box<dyn Stream<Item = Result<RaftMessage, Status>> + Send + 'static>>;

    async fn run(&self, request: Request<tonic::Streaming<RaftMessage>>) -> Result<Response<Self::RunStream>, Status> {
        unimplemented!();
    }
}