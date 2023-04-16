use anyhow::{Context, Error};
use crossbeam_channel::{unbounded, Receiver, Sender};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::fmt::Debug;
use std::io::{stdin, stdout, BufRead, Write};

/// The ID of a node in a Maelstrom network
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeId(pub String);

/// Request handler for a `Node`
pub trait Handler<T>: Fn(&mut NodeNet<T>, &mut T, &Request) -> Result<(), Error> {}
impl<F, T> Handler<T> for F where F: Fn(&mut NodeNet<T>, &mut T, &Request) -> Result<(), Error> {}

/// Custom event handler a `Node`
pub trait CustomEventHandler<T, C>: Fn(&mut NodeNet<T>, &mut T, C) -> Result<(), Error> {}
impl<F, T, C> CustomEventHandler<T, C> for F where
    F: Fn(&mut NodeNet<T>, &mut T, C) -> Result<(), Error>
{
}

/// Init handler for a `Node`
pub trait InitHandler<T>: Fn(&mut T, &NodeId, &[NodeId]) {}
impl<F, T> InitHandler<T> for F where F: Fn(&mut T, &NodeId, &[NodeId]) {}

/// Reply handler for a `Node` when it calls `send`
pub trait ReplyHandler<T>: Fn(&mut T) -> Result<(), Error> {}
impl<F, T> ReplyHandler<T> for F where F: Fn(&mut T) -> Result<(), Error> {}

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
    fn handle_init(&mut self, request: &InitRequest) -> Result<(), Error> {
        let request = &request.0;
        let msg: InitMessage = request.from_data()?;
        self.id = msg.node_id;
        self.node_ids = msg.node_ids;
        self.ack(request)
    }

    /// Reply to a `request` with a simple acknowledgement
    pub fn ack(&mut self, request: &Request) -> Result<(), Error> {
        self.reply(request, Map::new())
    }

    /// Reply to a `request` with a `body`
    pub fn reply<B: Serialize + Debug>(&mut self, request: &Request, body: B) -> Result<(), Error> {
        let mut out = stdout();
        self.reply_out(request, body, &mut out)
    }

    /// Send `body` to destination node
    pub fn send<B: Serialize + Debug, F: ReplyHandler<T> + 'static>(
        &mut self,
        dest: &NodeId,
        msg_type: &str,
        body: B,
        cb: F,
    ) -> Result<(), Error> {
        let mut out = stdout();
        let send_body = SendBody {
            msg_id: self.increment_msg_id(),
            msg_type,
            body,
        };
        self.replies.insert(send_body.msg_id, Box::new(cb));
        eprintln!("Send {:?}", send_body);
        eprintln!("Registered rplies {:?}", self.replies.keys());
        self.send_out(dest, send_body, &mut out)
    }

    /// Handles reply to this node from the network
    ///
    /// Calls a reply calllback if it was previously registered
    fn handle_reply(&mut self, request: &ReplyRequest, state: &mut T) -> Result<(), Error> {
        let in_reply_to = request
            .0
            .body
            .in_reply_to
            .expect("reply request missing in_reply_to");
        if let Some(cb) = self.replies.get(&in_reply_to) {
            cb(state)?;
        }
        self.replies.remove(&in_reply_to);
        Ok(())
    }

    /// Increment global message id for the node
    fn increment_msg_id(&mut self) -> u64 {
        self.current_msg_id += 1;
        self.current_msg_id
    }

    /// Reply to a `request` with a `body`, specifying output writer
    fn reply_out<B: Serialize + Debug, W: Write>(
        &mut self,
        request: &Request,
        body: B,
        out: &mut W,
    ) -> Result<(), Error> {
        let reply_body = ReplyBody {
            msg_id: self.increment_msg_id(),
            msg_type: request.body.msg_type.clone() + "_ok",
            in_reply_to: request.body.msg_id,
            body,
        };
        let dest = &request.src;
        self.send_out(dest, reply_body, out)
    }

    /// Send `body` to destination node, specifying output writer
    fn send_out<B: Serialize + Debug, W: Write>(
        &self,
        dest: &NodeId,
        body: B,
        out: &mut W,
    ) -> Result<(), Error> {
        let src = &self.id;
        let response = Response { src, dest, body };
        let json = serde_json::to_string(&response)?;

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

    fn handle(
        &mut self,
        request: &Request,
        net: &mut NodeNet<T>,
        state: &mut T,
    ) -> Result<(), Error> {
        let msg_type = &request.body.msg_type;
        match self.0.get(msg_type) {
            None => Ok(eprintln!("Unrecognized request type: {}", msg_type)),
            Some(h) => h(net, state, request),
        }
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

    fn handle(&mut self, net: &mut NodeNet<T>, state: &mut T, event: C) -> Result<(), Error> {
        let h = &self.handler;
        h(net, state, event)
    }
}

/// Internal events
enum Event<C> {
    Request(Request),
    Init(InitRequest),
    Reply(ReplyRequest),
    Custom(C),
    Stop,
}

/// An initialization request
struct InitRequest(Request);

/// A reply request received from the Maelstrom network
struct ReplyRequest(Request);

/// A request from the Maelstrom network
#[derive(Deserialize, Debug)]
pub struct Request {
    pub src: NodeId,
    pub dest: NodeId,
    pub body: RequestBody,
}

impl Request {
    /// Construct type from desierialized data of request
    ///
    /// Useful for implementing deserializable types that represent
    /// the payload of a particular request
    pub fn from_data<T: DeserializeOwned>(&self) -> Result<T, Error> {
        let value = self.body.data.clone();
        let v: Value = value.into();
        serde_json::from_value(v).context(format!(
            "deserializing request data failed: {:?}",
            self.body.data
        ))
    }
}
impl<C> From<Request> for Event<C> {
    fn from(value: Request) -> Self {
        if value.body.msg_type == "init" {
            Event::Init(InitRequest(value))
        } else if value.body.in_reply_to.is_some() {
            Event::Reply(ReplyRequest(value))
        } else {
            Event::Request(value)
        }
    }
}

/// A request body
#[derive(Deserialize, Debug)]
pub struct RequestBody {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub msg_id: Option<u64>,
    pub in_reply_to: Option<u64>,
    #[serde(flatten)]
    pub data: Map<String, Value>,
}

/// A response from the Maelstrom network
#[derive(Serialize, Debug)]
struct Response<'a, T: Serialize> {
    src: &'a NodeId,
    dest: &'a NodeId,
    body: T,
}

/// A `reply` body originating from this node
#[derive(Serialize, Debug)]
struct ReplyBody<T: Serialize> {
    #[serde(rename = "type")]
    msg_type: String,
    msg_id: u64,
    in_reply_to: Option<u64>,
    #[serde(flatten)]
    body: T,
}

/// A `send` body
#[derive(Serialize, Debug)]
struct SendBody<'a, T: Serialize> {
    #[serde(rename = "type")]
    msg_type: &'a str,
    msg_id: u64,
    #[serde(flatten)]
    body: T,
}

/// The body of an `InitRequest`
#[derive(Deserialize, Debug)]
struct InitMessage {
    node_id: NodeId,
    node_ids: Vec<NodeId>,
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
                let res = match input.read_line(&mut buffer) {
                    // Ok(0) is EOF
                    Ok(0) | Err(_) => input_sender.send(Event::Stop),
                    Ok(_) => {
                        let request = serde_json::from_slice::<Request>(buffer.as_bytes())
                            .expect("could not deserialize request");
                        eprintln!("Received request: {:?}", request);

                        input_sender.send(request.into())
                    }
                };

                if res.is_err() {
                    break;
                }
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

        fn log_if_error(result: Result<(), Error>, context: &str) {
            match result {
                Ok(()) => (),
                Err(err) => eprintln!("Error handling {}: {}", context, err),
            }
        }

        // main loop for handling events in the node
        for event in &event_receiver {
            match event {
                Event::Stop => break,
                Event::Custom(c) => log_if_error(
                    self.custom_events
                        .as_mut()
                        .expect("custom event received but no custom events registered")
                        .handle(&mut self.net, &mut self.state, c),
                    "custom event",
                ),
                Event::Init(request) => {
                    log_if_error(self.net.handle_init(&request), "initialization")
                }
                Event::Reply(request) => {
                    log_if_error(self.net.handle_reply(&request, &mut self.state), "reply")
                }
                Event::Request(request) => log_if_error(
                    self.routes.handle(&request, &mut self.net, &mut self.state),
                    &format!("request with type {}", request.body.msg_type),
                ),
            }
        }
    }
}

/// Tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_routes() -> Result<(), Error> {
        let mut node: Node<String, ()> =
            Node::new("".to_string()).route("echo", |_n, state, request| {
                state.push_str(&format!("{} handled", request.body.msg_type));
                Ok(())
            });
        let request = Request {
            src: NodeId("c1".to_string()),
            dest: NodeId("n1".to_string()),
            body: RequestBody {
                msg_type: "echo".to_string(),
                msg_id: None,
                in_reply_to: None,
                data: Map::new(),
            },
        };
        node.routes
            .handle(&request, &mut node.net, &mut node.state)?;
        assert_eq!("echo handled".to_string(), *node.state);
        Ok(())
    }

    #[test]
    fn test_reply() -> Result<(), Error> {
        let mut node: Node<(), ()> = Node::new(());
        node.net.id = NodeId("n1".to_string());
        let req = Request {
            src: NodeId("c1".to_string()),
            dest: node.net.id.clone(),
            body: RequestBody {
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
        node.net.reply_out(&req, Stuff { hello: 42 }, &mut out)?;
        let expected = r#"{"src":"n1","dest":"c1","body":{"type":"test_ok","msg_id":1,"in_reply_to":1,"hello":42}}"#
            .as_bytes();
        assert_eq!(expected, &out[..(out.len() - 1)]);

        Ok(())
    }
}
