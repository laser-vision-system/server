use std::time::{Duration, Instant};

use actix_ws::{AggregatedMessage, CloseReason};
use datatypes::shared::ConnId;
use futures_util::StreamExt as _;
use serde::Serialize;
use serde_json::Result;
use server_edge_datatypes as datatypes;
use server_edge_datatypes::gui::ServerRequests;
use tokio::{sync::mpsc, time::interval};

use crate::{ServerHandle, WSSide};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(2);
const CLIENT_TIMEOUT: Duration = Duration::from_secs(5);

// generic websocket handler - can process both edge and GUI websockets,
// although it may not be generic enough to handle images.
pub async fn ws_handler(
	server: ServerHandle,
	mut session: actix_ws::Session,
	msg_stream: actix_ws::MessageStream,
	side: WSSide,
) {
	let mut last_heartbeat = Instant::now();
	let mut interval = interval(HEARTBEAT_INTERVAL);

	let (conn_tx, mut conn_rx) = mpsc::unbounded_channel();

	let (img_tx, mut img_rx) = mpsc::unbounded_channel();

	let send_img_tx;

	match side {
		WSSide::Edge => {
			send_img_tx = None;
		},

		WSSide::GUI => {
			send_img_tx = Some(img_tx);
		},
	}

	let mut conn_id = server.connect(conn_tx, side, send_img_tx).await;
	let mut initialising = true;

	match side {
		WSSide::GUI => {
			println!("GUI connected: {conn_id}");
		},

		WSSide::Edge => {
			println!("Edge connected: {conn_id}");
		},
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
						process_ws(&server, &text, &mut conn_id, side, &mut initialising)
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
						println!("Sent to GUI: {chat_msg}");
					}

					WSSide::Edge => {
						println!("Sent to Edge: {chat_msg}");
					}
				}
				 session.text(chat_msg).await.unwrap();
			}

			Some(img_data) = img_rx.recv() => {
				session.binary(img_data).await.unwrap();
			}

			//keeps track of timeout conditions
			_ = interval.tick() => {
				if Instant::now().duration_since(last_heartbeat) > CLIENT_TIMEOUT {
						match side {
							WSSide::GUI => {
								println!("GUI timed out: {conn_id}");
							}

							WSSide::Edge => {
								println!("Edge timed out: {conn_id}");
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
			println!("GUI disconnected: {conn_id}");
		},

		WSSide::Edge => {
			println!("Edge disconnected: {conn_id}");
		},
	}

	server.disconnect(conn_id, side).await;

	let _ = session.close(close_reason).await;
}

async fn process_ws(
	server: &ServerHandle,
	text: &str,
	conn: &mut ConnId,
	side: WSSide,
	initialising: &mut bool,
) {
	match side {
		WSSide::Edge => {
			edge_process(server, text, conn, initialising).await;
		},
		WSSide::GUI => {
			gui_process(server, text, *conn).await;
		},
	}
	// this should contain the actual logic to deal with incoming text strings
	// from the websockets. Whether this needs to divide into edge and gui
	// functions right away is tbd
}

async fn gui_process(server: &ServerHandle, text: &str, conn: ConnId) {
	let msg = text.trim();
	if msg == "Ping" {
		return;
	}

	println!("Received from GUI: {msg}");

	let data_unchecked: Result<datatypes::gui::ServerRequests> =
		serde_json::from_str(msg);

	let Ok(data) = data_unchecked else {
		eprintln!(
			"Message could not deserialized due to: {:?}",
			data_unchecked.err().unwrap().to_string()
		);
		return;
	};

	match data {
		ServerRequests::ListEdges => server.list_edges(conn).await,

		ServerRequests::ListCameras => server.list_cameras(conn).await,

		ServerRequests::ViewCameraConfig(data) => {
			server.view_camera_config(conn, data).await
		},

		ServerRequests::Configuration(data) => {
			server.configuration(conn, data, WSSide::GUI).await
		},

		ServerRequests::SetEdgeName(data) => server.set_edge_name(data).await,

		ServerRequests::SetEdgeProcessingType(data) => {
			server.set_edge_processing_type(data).await
		},

		ServerRequests::SetCameraThresholds(data) => {
			server.set_camera_thresholds(data).await
		},

		ServerRequests::SetDefaultThreshold(data) => {
			server.set_default_thresholds(data).await
		},

		ServerRequests::GetDefaultThresholds => {
			server.get_default_thresholds(conn).await
		},

		ServerRequests::SetNameCamera(data) => {
			server.set_name_camera(data).await
		},

		ServerRequests::StartViewingCamera(data) => {
			server.start_viewing_camera(conn, data).await
		},

		ServerRequests::StopViewingCamera => {
			server.stop_viewing_camera(conn).await
		},

		ServerRequests::Setpoint(data) => server.setpoint(Some(data)).await,

		ServerRequests::ForgetCameraRequest(data) => {
			server.forget_camera(conn, data, WSSide::GUI).await
		},

		ServerRequests::ForceRetryCamera(data) => {
			server.force_retry_camera(data).await
		},

		ServerRequests::ListProcessingTypes => {
			server.list_processing_types(conn).await
		},

		_ => {
			eprintln!("Message not recognised");
		},
	}
}

async fn edge_process(
	server: &ServerHandle,
	text: &str,
	conn: &mut ConnId,
	initialising: &mut bool,
) {
	let msg = text.trim();

	println!("Received from Edge: {msg}");

	let data_unchecked: Result<datatypes::edge::ServerRequests> =
		serde_json::from_str(msg);

	let Ok(data) = data_unchecked else {
		eprintln!(
			"Message could not deserialized due to: {:?}",
			data_unchecked.err().unwrap().to_string()
		);
		return;
	};

	match data {
		datatypes::edge::ServerRequests::InitialiseConnection(data) => {
			if *initialising == false {
				eprintln!(
					"Already initialised connection tried initialising: {:?}",
					*conn
				);
				return;
			}

			let real_conn_id = data.edge_id;
			let success = server.initialise(*conn, data).await;

			if !success {
				eprintln!("Error initialising connection: {:?}", *conn);
			}

			*conn = real_conn_id;
			*initialising = false;
		},

		datatypes::edge::ServerRequests::RegisterCameras(data) => {
			server.register_cameras(*conn, data.cameras).await
		},

		datatypes::edge::ServerRequests::UpdateCameraStatus(data) => {
			server.update_camera_status(*conn, data).await
		},

		datatypes::edge::ServerRequests::ImageAnalysis(data) => {
			server.image_analysis(*conn, data).await
		},

		datatypes::edge::ServerRequests::Configuration(data) => {
			server.configuration(*conn, data, WSSide::Edge).await
		},

		datatypes::edge::ServerRequests::ForgetCameraRequest(data) => {
			server.forget_camera(*conn, data, WSSide::Edge).await
		},

		datatypes::edge::ServerRequests::Alert(data) => {
			server.alert_from_edge(*conn, data).await
		},

		_ => {
			eprintln!("Message not recognised");
		},
	}
}
