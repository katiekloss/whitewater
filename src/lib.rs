use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

pub struct RpcLifecycle {
    pub request: SetRequest,
    pub sender: oneshot::Sender<SetResponse>
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SetRequest {
    pub key: String,
    pub value: String
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SetResponse {
    pub message: String
}
