#![allow(dead_code, unused_variables)]
use gossip_glomers_rs::maelstrom::{
    Error, ErrorCode, KVService, MaelstromCode, Message, Node, NodeNet, Response, Result, KV,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, num::ParseIntError, str::FromStr};

/// Multi-node kafka-style append-only log

type StdResult<V, E> = std::result::Result<V, E>;

type State = ();

/// Log key
#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Clone, Debug)]
struct Key(String);

impl Key {
    /// The Maelstrom key-value key for the current offset of a key
    fn curr_offset(&self) -> String {
        format!("{}_curr_offset", self.0)
    }
    /// The Maelstrom key-value key for the message of a key at an offset
    fn message(&self, offset: Offset) -> String {
        format!("{}_offset_{}", self.0, offset.0)
    }
    /// The Maelstrom key-value key for the committed offset of a key
    fn committed(&self) -> String {
        format!("{}_committed_offset", self.0)
    }
}

/// Log offset
#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Copy, Debug, PartialOrd, Ord)]
struct Offset(isize);

impl Default for Offset {
    fn default() -> Self {
        Self(0)
    }
}

impl FromStr for Offset {
    type Err = ParseIntError;

    fn from_str(s: &str) -> StdResult<Self, Self::Err> {
        let raw = s.parse::<isize>()?;
        Ok(Offset(raw))
    }
}

impl Offset {
    /// The "unset" value for an offset. Should never be used
    /// as an actual offset.
    ///
    /// Useful for compare-and-swap when initializing
    fn unset() -> Self {
        Self(-1)
    }

    /// The starting value of an offset
    fn initial() -> Self {
        Self::default()
    }

    /// Successor
    fn succ(&self) -> Offset {
        Offset(self.0 + 1)
    }
}

/// Log message
// TODO: handle any type
#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Copy, Debug)]
struct Msg(u64);

impl FromStr for Msg {
    type Err = ParseIntError;

    fn from_str(s: &str) -> StdResult<Self, Self::Err> {
        let raw = s.parse::<u64>()?;
        Ok(Msg(raw))
    }
}

/// Increment the offset for a key by one in Maelstrom key-value store
fn increment<F>(net: &mut NodeNet<State>, key: Key, finished: F) -> Result
where
    F: Fn(&mut NodeNet<State>, StdResult<Offset, Error>) -> Result + 'static,
{
    let service = KVService::Seq;
    let curr_offset_key = key.curr_offset();

    let cas_cb = move |next: Offset, finished: F| {
        move |net: &mut NodeNet<State>, _state: &mut State, resp: Response| match resp {
            Ok(_) => finished(net, Ok(next)),
            Err(err) => match err.code {
                ErrorCode::Maelstrom(MaelstromCode::PreconditionFailed) => {
                    // try again!
                    increment(net, key, finished)
                }
                _ => finished(net, Err(err)),
            },
        }
    };

    net.read::<Offset>(
        service,
        &curr_offset_key.clone(),
        move |net, _state, resp| match resp {
            Ok(value) => {
                let next = value.succ();
                let create_if_not_exists = false;
                net.compare_and_swap(
                    service,
                    &curr_offset_key,
                    value,
                    next,
                    create_if_not_exists,
                    cas_cb(next, finished),
                )
            }
            Err(err) => match err.code {
                ErrorCode::Maelstrom(MaelstromCode::KeyDoesNotExist) => {
                    let unset = Offset::unset();
                    let initial = Offset::default();
                    let create_if_not_exists = true;
                    net.compare_and_swap(
                        service,
                        &curr_offset_key,
                        unset,
                        initial,
                        create_if_not_exists,
                        cas_cb(initial, finished),
                    )
                }
                _ => finished(net, Err(err)),
            },
        },
    )
}

/// Append key-msg in Maelstrom key-value store
fn append<F>(net: &mut NodeNet<State>, key: Key, msg: Msg, finished: F) -> Result
where
    F: Fn(&mut NodeNet<State>, StdResult<Offset, Error>) -> Result + Clone + 'static,
{
    increment(net, key.clone(), move |net, result| match result {
        Err(err) => finished(net, Err(err)),
        Ok(offset) => {
            let service = KVService::Seq;
            let message_key = key.message(offset);
            let finished = finished.clone();
            net.write(service, &message_key, msg, move |net, _, resp| {
                finished(net, resp.map(|_| offset))
            })
        }
    })
}

/// A NodeNet continuation
trait NodeNetCont<Acc>: FnOnce(&mut NodeNet<State>, StdResult<Acc, Error>) -> Result {}
impl<F, Acc> NodeNetCont<Acc> for F where
    F: FnOnce(&mut NodeNet<State>, StdResult<Acc, Error>) -> Result
{
}

/// Fold operation, with NodeNet continuation
trait FoldOpNodeNetCont<Acc, Val>:
    FnMut(&mut NodeNet<State>, Acc, Val, Box<dyn NodeNetCont<Acc>>) -> Result
{
}
impl<F, Acc, Val> FoldOpNodeNetCont<Acc, Val> for F where
    F: FnMut(&mut NodeNet<State>, Acc, Val, Box<dyn NodeNetCont<Acc>>) -> Result
{
}

/// Perform a fold over an iterator but with access to a NodeNet for network operations
/// and using a NodeNet continuation instead of return for accumulation
fn net_fold_cps<Iter, Acc, Val, Op, K>(
    net: &mut NodeNet<State>,
    mut iter: Iter,
    acc: Acc,
    mut op: Op,
    finished: K,
) -> Result
where
    Iter: Iterator<Item = Val> + 'static,
    Op: FoldOpNodeNetCont<Acc, Val> + Copy + 'static,
    K: NodeNetCont<Acc> + 'static,
{
    if let Some(value) = iter.next() {
        let cont = move |net: &mut NodeNet<State>, res: StdResult<Acc, Error>| match res {
            Err(err) => finished(net, Err(err)),
            Ok(acc) => net_fold_cps(net, iter, acc, op, finished),
        };
        let cont = Box::new(cont);
        op(net, acc, value, cont)
    } else {
        finished(net, Ok(acc))
    }
}

fn handle_send(net: &mut NodeNet<State>, _state: &mut State, message: &Message) -> Result {
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
    let reply_to_msg = message.clone();
    append(net, data.key, data.msg, move |net, result| match result {
        Ok(offset) => net.reply(&reply_to_msg, SendReplyData { offset }),
        Err(err) => net.reply_err(&reply_to_msg, err),
    })
}

fn handle_poll(net: &mut NodeNet<State>, _state: &mut State, message: &Message) -> Result {
    #[derive(Deserialize, Debug)]
    struct PollData {
        offsets: HashMap<Key, Offset>,
    }

    #[derive(Serialize, Debug)]
    struct PollReplyData {
        msgs: HashMap<Key, Vec<(Offset, Msg)>>,
    }

    type Msgs = HashMap<Key, Vec<(Offset, Msg)>>;
    fn read_msg(
        net: &mut NodeNet<State>,
        mut acc: Msgs,
        entry: (Key, Offset),
        cont: Box<dyn NodeNetCont<Msgs>>,
    ) -> Result {
        let service = KVService::Seq;
        let (key, offset) = entry;
        let msg_key = key.message(offset);
        net.read::<Msg>(service, &msg_key, move |net, _, res| match res {
            Ok(msg) => {
                acc.insert(key, vec![(offset, msg)]);
                cont(net, Ok(acc))
            }
            Err(err) => match err.code {
                ErrorCode::Maelstrom(MaelstromCode::KeyDoesNotExist) => cont(net, Ok(acc)),
                _ => cont(net, Err(err)),
            },
        })
    }

    let reply_to_msg = message.clone();
    let finished = move |net: &mut NodeNet<State>, res| match res {
        Ok(msgs) => net.reply(&reply_to_msg, PollReplyData { msgs }),
        Err(err) => net.reply_err(&reply_to_msg, err),
    };
    let data: PollData = message.parse_data()?;
    let iter = data.offsets.into_iter();
    net_fold_cps(net, iter, Msgs::new(), read_msg, finished)
}

fn handle_commit(net: &mut NodeNet<State>, state: &mut State, message: &Message) -> Result {
    #[derive(Deserialize)]
    struct CommitData {
        offsets: HashMap<Key, Offset>,
    }

    fn commit_msg(
        net: &mut NodeNet<State>,
        acc: (),
        entry: (Key, Offset),
        cont: Box<dyn NodeNetCont<()>>,
    ) -> Result {
        let (key, offset) = entry;
        let msg_key = key.committed();
        // trust client and just write value
        // TODO: it would add overhead but perhaps reading committed offset
        // and verifying we are not going backwards is a good idea (?)
        net.write(KVService::Seq, &msg_key, offset, move |net, _, res| {
            cont(net, res.map(|_| ()))
        })
    }
    let reply_to_msg = message.clone();
    let finished = move |net: &mut NodeNet<State>, res| match res {
        Ok(msgs) => net.ack(&reply_to_msg),
        Err(err) => net.reply_err(&reply_to_msg, err),
    };
    let data: CommitData = message.parse_data()?;
    let iter = data.offsets.into_iter();
    net_fold_cps(net, iter, (), commit_msg, finished)
}

fn handle_list(net: &mut NodeNet<State>, state: &mut State, message: &Message) -> Result {
    #[derive(Deserialize)]
    struct ListData {
        keys: Vec<Key>,
    }

    type Offsets = HashMap<Key, Offset>;
    #[derive(Serialize, Debug)]
    struct ListReplyData {
        offsets: Offsets,
    }

    fn read_committed(
        net: &mut NodeNet<State>,
        mut acc: Offsets,
        key: Key,
        cont: Box<dyn NodeNetCont<Offsets>>,
    ) -> Result {
        let msg_key = key.committed();
        net.read::<Offset>(KVService::Seq, &msg_key, move |net, _, res| match res {
            Ok(offset) => {
                acc.insert(key, offset);
                cont(net, Ok(acc))
            }
            Err(err) => match err.code {
                ErrorCode::Maelstrom(MaelstromCode::KeyDoesNotExist) => cont(net, Ok(acc)),
                _ => cont(net, Err(err)),
            },
        })
    }
    let reply_to_msg = message.clone();
    let finished = move |net: &mut NodeNet<State>, res| match res {
        Ok(offsets) => net.reply(&reply_to_msg, ListReplyData { offsets }),
        Err(err) => net.reply_err(&reply_to_msg, err),
    };
    let data: ListData = message.parse_data()?;
    let iter = data.keys.into_iter();
    net_fold_cps(net, iter, Offsets::new(), read_committed, finished)
}

fn main() {
    let mut node = Node::<State, ()>::default()
        .route("send", handle_send)
        .route("poll", handle_poll)
        .route("commit_offsets", handle_commit)
        .route("list_committed_offsets", handle_list);
    node.start();
}
