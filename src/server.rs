use tokio::sync::{mpsc, oneshot};

use std::{
    collections::{HashMap, HashSet},
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use crate::{ConnId, Msg};

enum Command {
    Connect {
        conn_tx: mpsc::UnboundedSender<Msg>,
        res_tx: oneshot::Sender<ConnId>
    },

    Disconnect {
        conn: ConnId
    }
}

#[derive(Debug)]
pub struct Server {
    gui_sessions: HashMap<ConnId, mpsc::UnboundedSender<Msg>>,

    edge_sessions: HashMap<ConnId, mpsc::UnboundedSender<Msg>>,

    visitor_count: Arc<AtomicUsize>,

    cmd_rx: mpsc::UnboundedReceiver<Msg>
}

impl Server {
    pub fn new() -> (Self, ServerHandle) {
        let (tx, rx) = mpsc::unbounded_channel();

        (
            Self {
                gui_sessions: HashMap::new(),
                edge_sessions: HashMap::new(),
                visitor_count: Arc::new(AtomicUsize::new(0)),
                cmd_rx: rx
            },
            ServerHandle { cmd_tx: tx },
        )
    }
}

#[derive(Debug, Clone)]
pub struct ServerHandle {
    cmd_tx: mpsc::UnboundedSender<Msg>
}