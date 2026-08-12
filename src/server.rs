use tokio::sync::{mpsc, oneshot};
use rand;
use std::{
    collections::{HashMap, HashSet},
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use rand::RngExt;
use crate::{ConnId, Msg, WSSide};

#[derive(Debug)]
enum Command {
    Connect {
        conn_tx: mpsc::UnboundedSender<Msg>,
        res_tx: oneshot::Sender<ConnId>,
        side: WSSide
    },

    Disconnect {
        conn: ConnId,
        side: WSSide
    },
}

#[derive(Debug)]
pub struct Server {
    gui_sessions: HashMap<ConnId, mpsc::UnboundedSender<Msg>>,

    edge_sessions: HashMap<ConnId, mpsc::UnboundedSender<Msg>>,

    visitor_count: Arc<AtomicUsize>,

    cmd_rx: mpsc::UnboundedReceiver<Command>
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

    async fn connect(&mut self, tx: mpsc::UnboundedSender<Msg>, side: WSSide) -> ConnId {
        let id = rand::rng().random::<ConnId>();
        match side {
            WSSide::GUI => {
                self.gui_sessions.insert(id, tx);
            }
            WSSide::Edge => {
                self.edge_sessions.insert(id, tx);
            }
        }
        let count = self.visitor_count.fetch_add(1, Ordering::SeqCst);

        id
    }

    async fn disconnect(&mut self, conn_id: ConnId, side: WSSide) {
        match side {
            WSSide::Edge => {
                self.edge_sessions.remove(&conn_id);
            }
            WSSide::GUI => {
                self.gui_sessions.remove(&conn_id);
            }
        }
    }

    pub async fn run(mut self) -> io::Result<()> {
        while let Some(cmd) = self.cmd_rx.recv().await {
            match cmd {
                Command::Connect { conn_tx, res_tx, side} => {
                    let conn_id = self.connect(conn_tx, side).await;
                    let _ = res_tx.send(conn_id);
                }

                Command::Disconnect { conn, side } => {
                    self.disconnect(conn,side).await;
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ServerHandle {
    cmd_tx: mpsc::UnboundedSender<Command>
}

impl ServerHandle {
    pub async fn connect(&self, conn_tx: mpsc::UnboundedSender<Msg>, side: WSSide) -> ConnId {
        let (res_tx, res_rx) = oneshot::channel();

        self.cmd_tx
            .send(Command::Connect { conn_tx, res_tx, side })
            .unwrap();

        res_rx.await.unwrap()
    }

    pub async fn disconnect(&self, conn: ConnId, side: WSSide) {
        self.cmd_tx.send(Command::Disconnect {conn,side}).unwrap();
    }
}