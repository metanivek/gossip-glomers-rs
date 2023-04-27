use gossip_glomers_rs::maelstrom::{Message, Node, NodeNet, Result};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
struct EchoData {
    echo: String,
}

fn handle_echo<T>(net: &mut NodeNet<T>, _state: &mut T, message: &Message) -> Result {
    let data: EchoData = message.parse_data()?;
    net.reply(message, data)
}

fn main() {
    let mut node: Node<_, ()> = Node::default().route("echo", handle_echo);
    node.start();
}
