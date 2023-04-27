use crossbeam_channel::{unbounded, Receiver, Sender};
use num_enum::{IntoPrimitive, TryFromPrimitive};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt::{Debug, Display};
use std::io::{stdin, stdout, BufRead, Write};

/// Maelstrom errors
/// https://github.com/jepsen-io/maelstrom/blob/main/resources/errors.edn
#[repr(u16)]
#[derive(Debug, Clone, Copy, Eq, PartialEq, IntoPrimitive, TryFromPrimitive)]
pub enum MaelstromCode {
    /// Indicates that the requested operation could not be completed within a timeout.
    Timeout = 0,
    /// Thrown when a client sends an RPC request to a node which does not exist.
    NodeNotFound = 1,
    /// "Use this error to indicate that a requested operation is not supported by
    /// the current implementation. Helpful for stubbing out APIs during development.
    NotSupported = 10,
    /// Indicates that the operation definitely cannot be performed at this time--perhaps
    /// because the server is in a read-only state, has not yet been initialized, believes
    /// its peers to be down, and so on. Do *not* use this error for indeterminate cases,
    /// when the operation may actually have taken place.
    TemporarilyUnavailable = 11,
    /// The client's request did not conform to the server's expectations, and could not
    /// possibly have been processed.
    MalformedRequest = 12,
    /// Indicates that some kind of general, indefinite error occurred. Use this
    /// as a catch-all for errors you can't otherwise categorize, or as a starting
    /// point for your error handler: it's safe to return `crash` for every
    /// problem by default, then add special cases for more specific errors later.
    Crash = 13,
    /// Indicates that some kind of general, definite error occurred. Use this as a
    /// catch-all for errors you can't otherwise categorize, when you specifically
    /// know that the requested operation has not taken place. For instance, you might
    /// encounter an indefinite failure during the prepare phase of a transaction: since
    /// you haven't started the commit process yet, the transaction can't have taken place.
    /// It's therefore safe to return a definite `abort` to the client.
    Abort = 14,
    /// The client requested an operation on a key which does not exist (assuming the
    /// operation should not automatically create missing keys).
    KeyDoesNotExist = 20,
    /// The client requested the creation of a key which already exists, and the server will
    /// not overwrite it.
    KeyAlreadyExists = 21,
    /// The requested operation expected some conditions to hold, and those conditions
    /// were not met. For instance, a compare-and-set operation might assert that the
    /// value of a key is currently 5; if the value is 3, the server would return
    /// `precondition-failed`.
    PreconditionFailed = 22,
    /// The requested transaction has been aborted because of a conflict with another
    /// transaction. Servers need not return this error on every conflict: they may choose
    /// to retry automatically instead.
    TxnConflict = 30,
}

impl<C> From<MaelstromCode> for Event<C> {
    fn from(value: MaelstromCode) -> Self {
        Event::MaelstromError(value)
    }
}

impl Display for MaelstromCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use MaelstromCode::*;
        let msg = match self {
            Timeout => "request timed out",
            NodeNotFound => "node does not exist",
            NotSupported => "request not supported",
            TemporarilyUnavailable => "node unavailable",
            MalformedRequest => "malformed request",
            Crash => "node crash with unspecified, indefinite error",
            Abort => "node abort with unspecified, definite error",
            KeyDoesNotExist => "key does not exist",
            KeyAlreadyExists => "key already exists",
            PreconditionFailed => "precondition failed",
            TxnConflict => "transation aborted because of conflict",
        };
        let code = *self as u16;

        write!(f, "{} ({})", msg, code)
    }
}

#[derive(Debug)]
pub enum ErrorCode {
    Maelstrom(MaelstromCode),
    Custom(u16),
}

impl From<ErrorCode> for u16 {
    fn from(value: ErrorCode) -> Self {
        use ErrorCode::*;

        match value {
            Maelstrom(c) => c.into(),
            Custom(c) => c,
        }
    }
}

impl From<u16> for ErrorCode {
    fn from(value: u16) -> Self {
        match MaelstromCode::try_from(value) {
            Ok(v) => ErrorCode::Maelstrom(v),
            Err(_) => ErrorCode::Custom(value),
        }
    }
}

impl Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use ErrorCode::*;
        match self {
            Maelstrom(c) => write!(f, "{}", c),
            Custom(c) => write!(f, "custom error ({})", c),
        }
    }
}

#[derive(Debug)]
pub struct Error {
    code: ErrorCode,
    text: Option<String>,
    source: Option<Box<dyn std::error::Error + 'static>>,
    // TODO: support extra key-value data in an error
}

impl Error {
    pub fn new(code: ErrorCode) -> Self {
        Self {
            code,
            text: None,
            source: None,
        }
    }

    pub fn custom(code: u16) -> Self {
        Self::new(ErrorCode::Custom(code))
    }

    pub fn maelstrom(code: MaelstromCode) -> Self {
        Self::new(ErrorCode::Maelstrom(code))
    }

    pub fn text(mut self, text: &str) -> Self {
        self.text = Some(text.to_owned());
        self
    }

    pub fn source<E>(mut self, source: E) -> Self
    where
        E: std::error::Error + 'static,
    {
        self.source = Some(Box::new(source));
        self
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let text_part = self
            .text
            .as_ref()
            .map_or("".to_owned(), |t| format!(" | {}", t));
        write!(f, "{}{}", self.code, text_part)
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_deref()
    }
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::maelstrom(MaelstromCode::Crash).source(value)
    }
}

impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        Self::maelstrom(MaelstromCode::Crash).source(value)
    }
}

/// An error message body
#[derive(Deserialize, Serialize, Debug)]
struct ErrorData {
    code: u16,
    text: Option<String>,
}

impl From<Error> for ErrorData {
    fn from(value: Error) -> Self {
        Self {
            code: value.code.into(),
            text: value.text,
        }
    }
}

pub type Result = std::result::Result<(), Error>;

/// Simple log trait to make some code a little nicer
trait Log {
    fn log(&self, ctx: &str);
}

impl Log for Result {
    fn log(&self, ctx: &str) {
        if let Err(e) = self {
            eprintln!("{} error'd: {}", ctx, e)
        }
    }
}

/// The ID of a node in a Maelstrom network
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeId(pub String);

/// Request handler for a `Node`
pub trait Handler<T>: Fn(&mut NodeNet<T>, &mut T, &Message) -> Result {}
impl<F, T> Handler<T> for F where F: Fn(&mut NodeNet<T>, &mut T, &Message) -> Result {}

/// Custom event handler a `Node`
pub trait CustomEventHandler<T, C>: Fn(&mut NodeNet<T>, &mut T, C) -> Result {}
impl<F, T, C> CustomEventHandler<T, C> for F where F: Fn(&mut NodeNet<T>, &mut T, C) -> Result {}

/// Reply handler for a `Node` when it calls `send`
pub trait ReplyHandler<T>: Fn(&mut T, &Response) -> Result {}
impl<F, T> ReplyHandler<T> for F where F: Fn(&mut T, &Response) -> Result {}

/// A node capable of being run by maelstrom
pub struct Node<T, C>
where
    C: Send + 'static,
{
    state: Box<T>,
    net: NodeNet<T>,
    routes: Routes<T>,
    custom_events: Option<CustomEvents<T, C>>,
}

/// For interacting with the `Node`'s maelstrom network
pub struct NodeNet<T> {
    pub id: NodeId,
    pub node_ids: Vec<NodeId>,
    current_msg_id: u64,
    replies: HashMap<u64, Box<dyn ReplyHandler<T>>>,
}

impl<T> NodeNet<T> {
    fn new() -> Self {
        Self {
            id: NodeId("unknown".to_string()),
            node_ids: vec![],
            current_msg_id: 0,
            replies: HashMap::new(),
        }
    }

    /// Handles the maelstrom initialization message
    fn handle_init(&mut self, message: &Message) -> Result {
        let msg: InitData = message.parse_data()?;
        self.id = msg.node_id;
        self.node_ids = msg.node_ids;
        self.ack(message)
    }

    /// Reply to a `message` with a simple acknowledgement
    pub fn ack(&mut self, message: &Message) -> Result {
        self.reply(message, Map::new())
    }

    /// Reply to a `message` with a `body`
    pub fn reply<B: Serialize + Debug>(&mut self, message: &Message, body: B) -> Result {
        let mut out = stdout();
        self.reply_out(message, body, &mut out)
    }

    /// Reply to a `message` with an `error`
    pub fn reply_err(&mut self, message: &Message, error: Error) -> Result {
        let mut out = stdout();
        let data: ErrorData = error.into();
        let reply_body = MessageBody {
            msg_id: Some(self.increment_msg_id()),
            msg_type: "error".to_owned(),
            in_reply_to: message.body.msg_id,
            data,
        };
        let dest = &message.src;
        self.send_out(dest, reply_body, &mut out)
    }

    /// Send `body` to destination node
    pub fn send<B: Serialize + Debug, F: ReplyHandler<T> + 'static>(
        &mut self,
        dest: &NodeId,
        msg_type: &str,
        data: B,
        cb: F,
    ) -> Result {
        let mut out = stdout();
        let msg_id = self.increment_msg_id();
        let send_body = MessageBody {
            msg_id: Some(msg_id),
            in_reply_to: None,
            msg_type: msg_type.to_owned(),
            data,
        };
        self.replies.insert(msg_id, Box::new(cb));
        eprintln!("Send {:?}", send_body);
        eprintln!("Registered rplies {:?}", self.replies.keys());
        self.send_out(dest, send_body, &mut out)
    }

    /// Handles reply to this node from the network
    ///
    /// Calls a reply calllback if it was previously registered
    fn handle_reply(&mut self, message: &Message, in_reply_to: u64, state: &mut T) -> Result {
        if let Some(cb) = self.replies.remove(&in_reply_to) {
            let response: Response = message.clone().into();
            cb(state, &response)?;
        }
        Ok(())
    }

    /// Increment global message id for the node
    fn increment_msg_id(&mut self) -> u64 {
        self.current_msg_id += 1;
        self.current_msg_id
    }

    /// Reply to a `message` with a `body`, specifying output writer
    fn reply_out<D: Serialize + Debug, W: Write>(
        &mut self,
        message: &Message,
        data: D,
        out: &mut W,
    ) -> Result {
        let reply_body = MessageBody {
            msg_id: Some(self.increment_msg_id()),
            msg_type: message.body.msg_type.clone() + "_ok",
            in_reply_to: message.body.msg_id,
            data,
        };
        let dest = &message.src;
        self.send_out(dest, reply_body, out)
    }

    /// Send `body` to destination node, specifying output writer
    fn send_out<B: Serialize + Debug, W: Write>(
        &self,
        dest: &NodeId,
        body: MessageBody<B>,
        out: &mut W,
    ) -> Result {
        let src = self.id.clone();
        let message = Message {
            src,
            dest: dest.clone(),
            body,
        };
        let json = serde_json::to_string(&message)?;

        eprintln!("Send JSON: {}", &json);
        out.write_all((json + "\n").as_bytes())?;
        out.flush()?;

        Ok(())
    }
}

/// Registered routes for a `Node`
struct Routes<T>(HashMap<String, Box<dyn Handler<T>>>);

impl<T> Routes<T> {
    fn new() -> Self {
        Self(HashMap::new())
    }

    fn add<F>(&mut self, message_type: &str, handler: F)
    where
        F: Handler<T> + 'static,
    {
        self.0.insert(message_type.to_string(), Box::new(handler));
    }

    fn handle(&mut self, message: &Message, net: &mut NodeNet<T>, state: &mut T) -> Result {
        let msg_type = &message.body.msg_type;
        let handler = self
            .0
            .get(msg_type)
            .ok_or_else(|| Error::maelstrom(MaelstromCode::NotSupported))?;
        handler(net, state, message)
    }
}

/// Registered custom events for a `Node`
struct CustomEvents<T, C> {
    receiver: Receiver<C>,
    handler: Box<dyn CustomEventHandler<T, C>>,
}

impl<T, C> CustomEvents<T, C> {
    fn new<F, H>(init: F, handler: H) -> Self
    where
        F: FnOnce(Sender<C>) + 'static,
        H: CustomEventHandler<T, C> + 'static,
    {
        let (sender, receiver) = unbounded();
        init(sender);
        Self {
            receiver,
            handler: Box::new(handler),
        }
    }

    fn handle(&mut self, net: &mut NodeNet<T>, state: &mut T, event: C) -> Result {
        let h = &self.handler;
        h(net, state, event)
    }
}

/// Internal events
enum Event<C> {
    Init(Message),
    MaelstromError(MaelstromCode),
    Request(Message),
    Reply(Message, u64),
    Custom(C),
    Stop,
}

/// The data of an initialization message
#[derive(Deserialize, Debug)]
struct InitData {
    node_id: NodeId,
    node_ids: Vec<NodeId>,
}

/// A message in the Maelstrom network
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Message<T = Map<String, Value>>
where
    T: Serialize,
{
    pub src: NodeId,
    pub dest: NodeId,
    pub body: MessageBody<T>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MessageBody<T: Serialize> {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub msg_id: Option<u64>,
    pub in_reply_to: Option<u64>,
    #[serde(flatten)]
    pub data: T,
}

impl Message {
    /// Parse body data into custom type `T`
    ///
    /// Useful for implementing deserializable types that represent
    /// the payload of a particular request
    pub fn parse_data<T: DeserializeOwned>(&self) -> std::result::Result<T, Error> {
        let value = self.body.data.clone();
        let v: Value = value.into();
        serde_json::from_value(v).map_err(|e| {
            Error::maelstrom(MaelstromCode::MalformedRequest)
                .source(e)
                .text("deserializing body data failed")
        })
    }
}

/// A response to a request originating from this node
pub type Response = std::result::Result<Message, Error>;

impl From<Message> for Response {
    fn from(value: Message) -> Self {
        if value.body.msg_type == "error" {
            Response::Err(value.parse_data::<ErrorData>().map_or_else(
                |err| {
                    Error::maelstrom(MaelstromCode::MalformedRequest)
                        .text("error body could not deserialize")
                        .source(err)
                },
                |body| Error {
                    code: body.code.into(),
                    text: body.text,
                    source: None,
                },
            ))
        } else {
            Response::Ok(value)
        }
    }
}

impl<C> From<Message> for Event<C> {
    fn from(value: Message) -> Self {
        if value.body.msg_type == "init" {
            Event::Init(value)
        } else if value.body.in_reply_to.is_some() {
            let in_reply_to = value.body.in_reply_to.unwrap();
            Event::Reply(value, in_reply_to)
        } else {
            Event::Request(value)
        }
    }
}

/// A default `Node` with `()` as its state
impl<C> Default for Node<(), C>
where
    C: Send + 'static,
{
    fn default() -> Self {
        Node::new(())
    }
}

impl<T, C> Node<T, C>
where
    C: Send + 'static,
{
    /// Create new node with specified state
    pub fn new(state: T) -> Self {
        Self {
            state: Box::new(state),
            net: NodeNet::new(),
            routes: Routes::new(),
            custom_events: None,
        }
    }

    /// Add route to a `Node` for handling a `message_type`
    pub fn route<F>(mut self, message_type: &str, handler: F) -> Self
    where
        F: Handler<T> + 'static,
    {
        self.routes.add(message_type, handler);
        self
    }

    /// Register a custom events sender
    ///
    /// Note: There can only be one for a node. Multiple calls to this method will panic.
    pub fn custom_events<F, H>(mut self, init: F, handler: H) -> Self
    where
        F: FnOnce(Sender<C>) + 'static,
        H: CustomEventHandler<T, C> + 'static,
    {
        if self.custom_events.is_some() {
            panic!("There is already a custom events sender registered.")
        }
        self.custom_events = Some(CustomEvents::new(init, handler));
        self
    }

    /// Start the main loop for a `Node`
    pub fn start(&mut self) {
        let (event_sender, event_receiver) = unbounded();
        let input_sender = event_sender.clone();

        // start thread to read from stdin
        std::thread::spawn(move || {
            let mut input = stdin().lock();
            let mut buffer = String::new();

            loop {
                buffer.clear();
                if (match input.read_line(&mut buffer) {
                    Ok(0) | Err(_) => {
                        // Ok(0) is EOF; Err is likely unrecoverable
                        // Send one last event to stop and intentionlly break
                        // our loop
                        let _ = input_sender.send(Event::Stop);
                        break;
                    }
                    Ok(_) => {
                        let bytes = buffer.as_bytes();
                        let event: Event<C> = match serde_json::from_slice::<Message>(bytes) {
                            Ok(r) => r.into(),
                            Err(_) => MaelstromCode::MalformedRequest.into(),
                        };
                        input_sender.send(event)
                    }
                })
                .is_err()
                {
                    // Error from input handling; break loop
                    break;
                };
            }
        });

        // start thread for custom events
        if let Some(c) = &self.custom_events {
            let recv = c.receiver.clone();
            std::thread::spawn(move || {
                for custom in recv {
                    if event_sender.send(Event::Custom(custom)).is_err() {
                        break;
                    }
                }
            });
        }

        // main loop for handling events in the node
        for event in &event_receiver {
            match event {
                Event::Stop => break,
                Event::Custom(c) => self
                    .custom_events
                    .as_mut()
                    .expect("custom event received but no custom events registered")
                    .handle(&mut self.net, &mut self.state, c)
                    .log("custom event handling"),
                Event::Init(message) => {
                    if let Err(e) = self.net.handle_init(&message) {
                        self.net
                            .reply_err(&message, e)
                            .log("init request err reply");
                    }
                }
                Event::Reply(message, in_reply_to) => {
                    if let Err(e) = self
                        .net
                        .handle_reply(&message, in_reply_to, &mut self.state)
                    {
                        self.net
                            .reply_err(&message, e)
                            .log("reply request err reply");
                    }
                }
                Event::Request(message) => {
                    eprintln!("Received request: {:?}", message);
                    if let Err(e) = self.routes.handle(&message, &mut self.net, &mut self.state) {
                        self.net.reply_err(&message, e).log("request err reply");
                    }
                }
                Event::MaelstromError(err) => {
                    eprintln!("Error without request: {}", err)
                }
            }
        }
    }
}

/// Tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_routes() -> Result {
        let mut node: Node<String, ()> =
            Node::new("".to_string()).route("echo", |_n, state, request| {
                state.push_str(&format!("{} handled", request.body.msg_type));
                Ok(())
            });
        let message = Message {
            src: NodeId("c1".to_string()),
            dest: NodeId("n1".to_string()),
            body: MessageBody {
                msg_type: "echo".to_string(),
                msg_id: None,
                in_reply_to: None,
                data: Map::new(),
            },
        };
        node.routes
            .handle(&message, &mut node.net, &mut node.state)?;
        assert_eq!("echo handled".to_string(), *node.state);
        Ok(())
    }

    #[test]
    fn test_reply() -> Result {
        let mut node: Node<(), ()> = Node::new(());
        node.net.id = NodeId("n1".to_string());
        let message = Message {
            src: NodeId("c1".to_string()),
            dest: node.net.id.clone(),
            body: MessageBody {
                msg_type: "test".to_string(),
                msg_id: Some(1),
                in_reply_to: None,
                data: Map::default(),
            },
        };

        #[derive(Serialize, Debug)]
        struct Stuff {
            hello: u32,
        }

        let mut out: Vec<u8> = Vec::new();
        node.net
            .reply_out(&message, Stuff { hello: 42 }, &mut out)?;
        let expected = r#"{"src":"n1","dest":"c1","body":{"type":"test_ok","msg_id":1,"in_reply_to":1,"hello":42}}"#
            .as_bytes();
        assert_eq!(expected, &out[..(out.len() - 1)]);

        Ok(())
    }
}
