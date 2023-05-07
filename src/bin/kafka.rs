use gossip_glomers_rs::maelstrom::{Message, Node, NodeNet, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Single node kafka-style append-only log

#[derive(Default)]
struct State {
    logs: HashMap<Key, Log>,
}

/// Appendable log with message offsets and committed offset level
#[derive(Default)]
struct Log {
    curr_offset: Offset,
    committed_offset: Option<Offset>,
    entries: Vec<(Offset, Msg)>,
}

impl Log {
    /// Append a message to the log and return its offset
    fn append(&mut self, msg: Msg) -> Offset {
        self.curr_offset.incr();
        self.entries.push((self.curr_offset, msg));
        self.curr_offset
    }

    /// Mark log as committed to an offset
    fn commit(&mut self, offset: Offset) {
        self.committed_offset = Some(offset)
    }

    /// Return limited number of messages with offset greater than equal than given offset
    fn poll(&self, offset: Offset, limit: usize) -> Vec<(Offset, Msg)> {
        self.entries
            .iter()
            .filter(|entry| entry.0 >= offset)
            .take(limit)
            .cloned()
            .collect()
    }
}

impl State {
    /// Append a message to the log and return its offset
    fn append(&mut self, key: Key, msg: Msg) -> Offset {
        let log = self.logs.entry(key).or_default();
        log.append(msg)
    }

    /// Marks a log as committed up to a given offset
    fn commit(&mut self, key: Key, offset: Offset) {
        self.logs.entry(key).and_modify(|log| log.commit(offset));
    }

    /// Returns some number of messages from a log, starting at a given offset
    fn poll(&self, key: &Key, offset: Offset) -> Vec<(Offset, Msg)> {
        match self.logs.get(key) {
            None => Vec::new(),
            Some(log) => log.poll(offset, 1),
        }
    }

    /// Returns the committed offset for a log
    fn committed(&self, key: &Key) -> Option<Offset> {
        match self.logs.get(key) {
            None => None,
            Some(log) => log.committed_offset,
        }
    }
}

/// Log key
#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Clone, Debug)]
struct Key(String);

/// Log offset
#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Copy, Debug, Default, PartialOrd, Ord)]
struct Offset(usize);

impl Offset {
    fn incr(&mut self) {
        self.0 += 1
    }
}

/// Log message
// TODO: handle any type
#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Copy, Debug)]
struct Msg(u64);

fn handle_send(net: &mut NodeNet<State>, state: &mut State, message: &Message) -> Result {
    #[derive(Deserialize)]
    struct SendData {
        key: Key,
        msg: Msg,
    }

    #[derive(Serialize, Debug)]
    struct SendReplyData {
        offset: Offset,
    }

    let data: SendData = message.parse_data()?;
    let offset = state.append(data.key, data.msg);
    net.reply(message, SendReplyData { offset })
}

fn handle_poll(net: &mut NodeNet<State>, state: &mut State, message: &Message) -> Result {
    #[derive(Deserialize)]
    struct PollData {
        offsets: HashMap<Key, Offset>,
    }

    #[derive(Serialize, Debug)]
    struct PollReplyData {
        msgs: HashMap<Key, Vec<(Offset, Msg)>>,
    }

    let data: PollData = message.parse_data()?;
    let msgs: HashMap<Key, Vec<(Offset, Msg)>> = data
        .offsets
        .into_iter()
        .map(|(key, offset)| {
            let msgs = state.poll(&key, offset);
            (key, msgs)
        })
        .collect();
    net.reply(message, PollReplyData { msgs })
}

fn handle_commit(net: &mut NodeNet<State>, state: &mut State, message: &Message) -> Result {
    #[derive(Deserialize)]
    struct CommitData {
        offsets: HashMap<Key, Offset>,
    }
    let data: CommitData = message.parse_data()?;

    for (key, offset) in data.offsets.into_iter() {
        state.commit(key, offset)
    }

    net.ack(message)
}

fn handle_list(net: &mut NodeNet<State>, state: &mut State, message: &Message) -> Result {
    #[derive(Deserialize)]
    struct ListData {
        keys: Vec<Key>,
    }

    #[derive(Serialize, Debug)]
    struct ListReplyData {
        offsets: HashMap<Key, Offset>,
    }

    let data: ListData = message.parse_data()?;

    let offsets: HashMap<Key, Offset> = data
        .keys
        .into_iter()
        .filter_map(|key| state.committed(&key).map(|offset| (key, offset)))
        .collect();

    net.reply(message, ListReplyData { offsets })
}

fn main() {
    let mut node = Node::<State, ()>::new(State::default())
        .route("send", handle_send)
        .route("poll", handle_poll)
        .route("commit_offsets", handle_commit)
        .route("list_committed_offsets", handle_list);
    node.start();
}
