use gossip_glomers_rs::maelstrom::{Node, NodeId, NodeNet, Request, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::thread;
use std::time::Duration;

type Topology = HashMap<String, Vec<NodeId>>;

#[derive(Serialize, Deserialize, Debug)]
struct TopologyMessage {
    topology: Topology,
}

#[derive(Serialize, Deserialize, Debug)]
struct BroadcastMessage {
    message: u32,
}

#[derive(Serialize, Debug)]
struct ReadResponse<'a> {
    messages: &'a Vec<u32>,
}

/// State for our broadcast nodes
#[derive(Default)]
struct State {
    topology: Option<Topology>,
    seen_messages: Vec<u32>,
    outgoing_messages: HashMap<NodeId, Vec<u32>>,
}

impl State {
    fn handle_broadcast(&mut self, net: &NodeNet<State>, msg: BroadcastMessage, src: &NodeId) {
        // Return if we already have this message
        // since we only want to send it out when we first
        // see it
        if self.seen_messages.contains(&msg.message) {
            return;
        }

        let message = msg.message;
        self.seen_messages.push(message);

        // Schedule for sync
        for id in net
            .node_ids
            .iter()
            .filter(|id| *id != &net.id && *id != src)
        {
            let msgs = self.outgoing_messages.entry(id.to_owned()).or_default();
            msgs.push(message)
        }
    }

    fn handle_sync(&self, net: &mut NodeNet<State>) -> Result {
        eprintln!("Outgoing messages: {:?}", self.outgoing_messages);
        for id in self.outgoing_messages.keys() {
            let msgs = self.outgoing_messages.get(id).unwrap();
            for msg in msgs {
                let broadcast = BroadcastMessage { message: *msg };
                {
                    let m = *msg;
                    let i = id.clone();
                    net.send(id, "broadcast", broadcast, move |s| {
                        eprintln!("received reply from {:?} for {:?}", i, m);
                        s.outgoing_messages
                            .entry(i.clone())
                            .and_modify(|msgs| msgs.retain(|msg| msg != &m));
                        Ok(())
                    })?;
                }
            }
        }
        Ok(())
    }
}

fn handle_topology(net: &mut NodeNet<State>, state: &mut State, request: &Request) -> Result {
    let msg: TopologyMessage = request.from_data()?;
    state.topology = Some(msg.topology);
    net.ack(request)
}

fn handle_broadcast(net: &mut NodeNet<State>, state: &mut State, request: &Request) -> Result {
    let msg: BroadcastMessage = request.from_data()?;
    state.handle_broadcast(net, msg, &request.src);
    net.ack(request)
}

fn handle_read(net: &mut NodeNet<State>, state: &mut State, request: &Request) -> Result {
    let read_response = ReadResponse {
        messages: &state.seen_messages,
    };
    net.reply(request, read_response)
}

enum BroadcastEvent {
    Sync,
}

fn main() {
    let state = State::default();
    let mut node: Node<State, BroadcastEvent> = Node::new(state)
        .route("topology", handle_topology)
        .route("broadcast", handle_broadcast)
        .route("read", handle_read)
        .custom_events(
            |sender| {
                thread::spawn(move || loop {
                    thread::sleep(Duration::from_millis(250));
                    if sender.send(BroadcastEvent::Sync).is_err() {
                        break;
                    }
                });
            },
            |n, s, e| match e {
                BroadcastEvent::Sync => s.handle_sync(n),
            },
        );
    node.start();
}
