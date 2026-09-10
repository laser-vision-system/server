
use std::time::{Duration, Instant};

use actix_ws::{AggregatedMessage, CloseReason};
use futures_util::StreamExt as _;
use tokio::{sync::mpsc, time::interval};

use crate::{ServerHandle, WSSide};
use server_edge_datatypes as datatypes;
use serde_json::Result;
use datatypes::shared::{ConnId};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(2);
const CLIENT_TIMEOUT: Duration = Duration::from_secs(5);

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

    let (img_tx, mut img_rx) = mpsc::unbounded_channel();

    let send_img_tx;

    match side {
        WSSide::Edge => {
            send_img_tx = None;
        }

        WSSide::GUI => {
            send_img_tx = Some(img_tx);
        }
    }

    let conn_id = server.connect(conn_tx, side, send_img_tx).await;

    match side {
        WSSide::GUI => {
            println!("GUI connected: {}", conn_id)
        }

        WSSide::Edge => {
            println!("Edge connected: {}", conn_id)
        }
    }

    let mut msg_stream = msg_stream
        .max_frame_size(20 * 1024 * 1024)
        .aggregate_continuations()
        .max_continuation_size(2 * 1024 * 1024);

    let close_reason: Option<CloseReason> = loop {
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
                        last_heartbeat = Instant::now();
                        process_ws(&server, &text, conn_id, side)
                            .await;
                    }

                    AggregatedMessage::Binary(bin) => {
                        match side {
                            WSSide::GUI => {
                                break None
                            }

                            WSSide::Edge => {
                                server.route_image(conn_id, bin).await;
                            }
                        }
                    }

                    AggregatedMessage::Close(reason) => break reason,

                }
            }

            //Sends any text input to the conn_tx sender back to the client
            Some(chat_msg) = conn_rx.recv() => {
                match side {
                    WSSide::GUI => {
                        println!("Sent to GUI: {}", chat_msg);
                    }

                    WSSide::Edge => {
                        println!("Sent to Edge: {}", chat_msg);
                    }
                }
                 session.text(chat_msg).await.unwrap();
            }

            Some(img_data) = img_rx.recv() => {
                println!("Routing image to: {:?}", conn_id);
                session.binary(img_data).await.unwrap();
            }

            //keeps track of timeout conditions
            _ = interval.tick() => {
                if Instant::now().duration_since(last_heartbeat) > CLIENT_TIMEOUT {
                        match side {
                            WSSide::GUI => {
                                println!("GUI timed out: {}", conn_id)
                            }

                            WSSide::Edge => {
                                println!("Edge timed out: {}", conn_id)
                            }
                        }
                    break None;
                }
                let _ = session.ping(b"").await;
            }

            else => {
                break None;
            }
        }
    };

    match side {
        WSSide::GUI => {
            println!("GUI disconnected: {}", conn_id)
        }

        WSSide::Edge => {
            println!("Edge disconnected: {}", conn_id)
        }
    }

    server.disconnect(conn_id, side).await;

    let _ = session.close(close_reason).await;
}

async fn process_ws(
    server: &ServerHandle,
    text: &str,
    conn: ConnId,
    side: WSSide
) {
    match side  {
        WSSide::Edge => {
            edge_process(server, text, conn).await;
        },
        WSSide::GUI => {
            gui_process(server, text, conn).await;
        }
    }
    //this should contain the actual logic to deal with incoming text strings from the websockets. Whether this needs to divide into edge and gui functions right away is tbd
}

async fn gui_process(
    server: &ServerHandle,
    text: &str,
    conn: ConnId
) {
    let msg = text.trim();

    println!("Received from GUI: {}", msg);

    let mut args = msg.splitn(2, ':');

    match args.next().unwrap() {
        "ListCameras"  => {
            server.list_cameras(conn).await;
        }

        "ListEdges" => {
            server.list_edges(conn).await;
        }
        "SetNameCamera"  => match args.next() {
            Some(data) => {
                let data_unchecked: Result<datatypes::gui::SetNameCamera> = serde_json::from_str(data);
                if data_unchecked.is_ok() {
                    let json = data_unchecked.unwrap();
                    server.set_name_camera(json).await;
                }
            }

            None => {}
        }

        "StartViewingCamera"  => match args.next() {
            Some(data) => {
                let data_unchecked: Result<datatypes::gui::StartViewingCamera> = serde_json::from_str(data);
                if data_unchecked.is_ok() {
                    let json = data_unchecked.unwrap();
                    server.start_viewing_camera(conn, json).await;
                }
            }

            None => {}
        }


        "StopViewingCamera"  => {
            server.stop_viewing_camera(conn).await;
        }

        "Setpoint" => match args.next() {
            Some(data) => {
                let data_unchecked: Result<datatypes::gui::Setpoint> = serde_json::from_str(data);
                if data_unchecked.is_ok()  {
                    let json = data_unchecked.unwrap();
                    server.setpoint(Some(json)).await;
                }
            }

            None => {
                server.setpoint(None).await;
            }
        }

        "ViewCameraConfig" => match args.next() {
            Some(data) => {
                let data_unchecked: Result<datatypes::shared::ViewCameraConfig> = serde_json::from_str(data);
                if data_unchecked.is_ok()  {
                    let json = data_unchecked.unwrap();
                    server.view_camera_config(conn, json).await;
                }
            }

            None => {}
        }

        "Configuration" => match args.next() {
            Some(data) => {
                let data_unchecked: Result<datatypes::shared::Configuration> = serde_json::from_str(data);
                if data_unchecked.is_ok()  {
                    let json = data_unchecked.unwrap();
                    server.configuration(conn, json, WSSide::GUI).await;
                }
            }

            None => {}
        }

        "ForgetCameraRequest" => match args.next() {
            Some(data) => {
                let data_unchecked: Result<datatypes::shared::ForgetCameraRequest> = serde_json::from_str(data);
                if data_unchecked.is_ok()  {
                    let json = data_unchecked.unwrap();
                    server.forget_camera(conn, json, WSSide::GUI).await;
                }
            }
            None => {}
        }

        "SetCameraThresholds" => match args.next() {
            Some(data) => {
                let data_unchecked: Result<datatypes::gui::SetCameraThresholds> = serde_json::from_str(data);
                if data_unchecked.is_ok()  {
                    let json = data_unchecked.unwrap();
                    server.set_camera_thresholds(json).await;
                }
            }
            None => {}
        }

        "SetDefaultThreshold" => match args.next() {
            Some(data) => {
                let data_unchecked: Result<datatypes::gui::SetDefaultThreshold> = serde_json::from_str(data);
                if data_unchecked.is_ok()  {
                    let json = data_unchecked.unwrap();
                    server.set_default_thresholds(json).await;
                }
            }

            None => {}
        }

        "SetEdgeProcessingType" => match args.next() {
            Some(data) => {
                let data_unchecked: Result<datatypes::gui::SetEdgeProcessingType> = serde_json::from_str(data);
                if data_unchecked.is_ok()  {
                    let json = data_unchecked.unwrap();
                    server.set_edge_processing_type(conn, json).await;
                }
            }

            None => {}
        }

        "GetDefaultThresholds"  =>  {
            server.get_default_thresholds(conn).await;
        }

        _ => {

        }
    }
}

async fn edge_process(
    server: &ServerHandle,
    text: &str,
    conn: ConnId
) {
    let msg = text.trim();

    println!("Received from Edge: {}", msg);

    let mut args = msg.splitn(2, ':');
    match args.next().unwrap() {

        "RegisterCameras" => match args.next() {
            Some(data) => {
                let data_unchecked: Result<datatypes::edge::RegisterCameras> = serde_json::from_str(data);
                if data_unchecked.is_ok()  {
                    let json = data_unchecked.unwrap();
                    server.register_cameras(conn, json.cameras).await;
                }
            }

            None => {

            }
        }

        "DisconnectCamera" => match args.next() {
            Some(data) => {
                let data_unchecked: Result<datatypes::edge::UpdateCameraStatus> = serde_json::from_str(data);
                if data_unchecked.is_ok()  {
                    let json = data_unchecked.unwrap();
                    server.unregister_camera(conn, json.camera_id).await;
                }
            }

            None => {

            }
        }

        "ImageAnalysis" => match args.next() {
            Some(data) => {
                let data_unchecked: Result<datatypes::edge::ImageAnalysis> = serde_json::from_str(data);
                if data_unchecked.is_ok()  {
                    let json = data_unchecked.unwrap();
                    server.image_analysis(conn, json).await;
                }
            }
            None => {

            }
        }

        "Configuration" => match args.next() {
            Some(data) => {
                let data_unchecked: Result<datatypes::shared::Configuration> = serde_json::from_str(data);
                if data_unchecked.is_ok()  {
                    let json = data_unchecked.unwrap();
                    server.configuration(conn, json, WSSide::Edge).await;
                }
            }

            None => {}
        }

        "ForgetCameraRequest" => match args.next() {
            Some(data) => {
                let data_unchecked: Result<datatypes::shared::ForgetCameraRequest> = serde_json::from_str(data);
                if data_unchecked.is_ok()  {
                    let json = data_unchecked.unwrap();
                    server.forget_camera(conn, json, WSSide::Edge).await;
                }
            }
            None => {}
        }

        _ => {

        }
    }
}