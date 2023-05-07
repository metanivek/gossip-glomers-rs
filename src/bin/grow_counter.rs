use gossip_glomers_rs::maelstrom::{
    ErrorCode, KVService, MaelstromCode, Message, Node, NodeNet, Result, KV,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
struct AddData {
    delta: u64,
}

#[derive(Serialize, Debug)]
struct ReadReplyData {
    value: u64,
}

const KEY: &str = "grow_counter";
const SERVICE: KVService = KVService::Seq;

fn increment<F>(net: &mut NodeNet<()>, delta: u64, finished: F) -> Result
where
    F: Fn(&mut NodeNet<()>, Result) -> Result + Clone + 'static,
{
    net.read::<u64>(SERVICE, KEY, move |net, _state, resp| match resp {
        Ok(value) => {
            let new_value = value + delta;
            let finished = finished.clone();
            net.compare_and_swap(
                SERVICE,
                KEY,
                value,
                new_value,
                false,
                move |net, _state, resp| match resp {
                    Ok(_) => finished(net, Ok(())),
                    Err(err) => match err.code {
                        ErrorCode::Maelstrom(MaelstromCode::PreconditionFailed) => {
                            // try again!
                            increment(net, delta, finished.clone())
                        }
                        _ => finished(net, Err(err)),
                    },
                },
            )
        }
        Err(err) => match err.code {
            ErrorCode::Maelstrom(MaelstromCode::KeyDoesNotExist) => {
                let finished = finished.clone();
                net.write(SERVICE, KEY, delta, move |net, _, resp| {
                    finished(net, resp.map(|_| ()))
                })
            }
            _ => finished(net, Err(err)),
        },
    })
}

fn handle_add(net: &mut NodeNet<()>, _state: &mut (), message: &Message) -> Result {
    let reply_to_msg = message.clone();
    let data: AddData = message.parse_data()?;

    increment(net, data.delta, move |net, result| match result {
        Ok(_) => net.ack(&reply_to_msg),
        Err(err) => net.reply_err(&reply_to_msg, err),
    })
}

fn handle_read<T>(net: &mut NodeNet<T>, _state: &mut T, message: &Message) -> Result {
    let reply_to_msg = message.clone();
    net.read::<u64>(SERVICE, KEY, move |net, _state, resp| match resp {
        Ok(value) => net.reply(&reply_to_msg, ReadReplyData { value }),
        Err(err) => net.reply_err(&reply_to_msg, err),
    })
}

fn main() {
    let mut node: Node<_, ()> = Node::default()
        .route("add", handle_add)
        .route("read", handle_read);
    node.start();
}
