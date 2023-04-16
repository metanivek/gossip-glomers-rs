use anyhow::Error;
use gossip_glomers_rs::maelstrom::{Node, NodeNet, Request};
use serde::Serialize;
use uuid::Uuid;

#[derive(Serialize, Debug)]
struct GenerateResponse {
    id: String,
}

fn handle_generate<T>(
    net: &mut NodeNet<T>,
    _state: &mut T,
    request: &Request,
) -> Result<(), Error> {
    let msg = GenerateResponse {
        id: Uuid::new_v4().to_string(),
    };
    net.reply(request, msg)
}

fn main() {
    let mut node: Node<_, ()> = Node::default().route("generate", handle_generate);
    node.start();
}
