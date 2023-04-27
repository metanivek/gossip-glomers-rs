use gossip_glomers_rs::maelstrom::{Message, Node, NodeNet, Result};
use serde::Serialize;
use uuid::Uuid;

#[derive(Serialize, Debug)]
struct GenerateData {
    id: String,
}

fn handle_generate<T>(net: &mut NodeNet<T>, _state: &mut T, message: &Message) -> Result {
    let data = GenerateData {
        id: Uuid::new_v4().to_string(),
    };
    net.reply(message, data)
}

fn main() {
    let mut node: Node<_, ()> = Node::default().route("generate", handle_generate);
    node.start();
}
