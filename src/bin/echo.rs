use gossip_glomers_rs::maelstrom::{Node, NodeNet, Request, Result};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
struct EchoMessage {
    echo: String,
}

fn handle_echo<T>(net: &mut NodeNet<T>, _state: &mut T, request: &Request) -> Result {
    let msg: EchoMessage = request.from_data()?;
    net.reply(request, msg)
}

fn main() {
    let mut node: Node<_, ()> = Node::default().route("echo", handle_echo);
    node.start();
}
