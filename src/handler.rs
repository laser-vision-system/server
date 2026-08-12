use std::time::{Duration, Instant};

use actix_ws::AggregatedMessage;
use futures_util::StreamExt as _;
use tokio::{sync::mpsc, time::interval};

use crate::{ServerHandle, ConnId, WSSide};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const CLIENT_TIMEOUT: Duration = Duration::from_secs(10);

//generic websocket handler - can process both edge and GUI websockets, although it may not be generic enough to handle images.
pub async fn ws_handler(
    server: ServerHandle,
    mut session: actix_ws::Session,
    msg_stream: actix_ws::MessageStream,
    side: WSSide
){
    let mut last_heartbeat = Instant::now();
    let mut interval = interval(HEARTBEAT_INTERVAL);

    let (conn_tx, mut conn_rx) = mpsc::unbounded_channel();

    let conn_id = server.connect(conn_tx, side).await;

    let mut msg_stream = msg_stream
        .max_frame_size(128 * 1024)
        .aggregate_continuations()
        .max_continuation_size(2 * 1024 * 1024);

    let close_reason = loop {
        tokio::select! { //waits for the first of the following events:
            //handles incoming ws traffic
            Some(Ok(msg)) = msg_stream.next() => {
                match msg {
                    AggregatedMessage::Ping(bytes) => {
                        last_heartbeat = Instant::now();
                        session.pong(&bytes).await.unwrap();
                    }

                    AggregatedMessage::Pong(_) => {
                        last_heartbeat = Instant::now();
                    }

                    AggregatedMessage::Text(text) => {
                        process_ws(&server, &mut session, &text, conn_id, side)
                            .await;
                    }

                    AggregatedMessage::Binary(_bin) => {
                        log::warn!("unexpected binary message");
                    }

                    AggregatedMessage::Close(reason) => break reason,

                }
            }

            //Sends any text input to the conn_tx sender back to the client
            Some(chat_msg) = conn_rx.recv() => {
                 session.text(chat_msg).await.unwrap();
            }

            //keeps track of timeout conditions
            _ = interval.tick() => {
                if Instant::now().duration_since(last_heartbeat) > CLIENT_TIMEOUT {
                    break None;
                }
                let _ = session.ping(b"").await;
            }

            else => {
                break None;
            }
        }
    };

    let _ = server.disconnect(conn_id, side);

    let _ = session.close(close_reason).await;
}

async fn process_ws(
    chat_server: &ServerHandle,
    session: &mut actix_ws::Session,
    text: &str,
    conn: ConnId,
    side: WSSide
) {
    //this should contain the actual logic to deal with incoming text strings from the websockets. Whether this needs to divide into edge and gui functions right away is tbd
}