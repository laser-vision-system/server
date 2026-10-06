use std::time::{Duration, Instant};

use actix_ws::{AggregatedMessage, CloseCode, CloseReason, Message};
use datatypes::shared::ConnId;
use futures_util::StreamExt as _;
use serde::Serialize;
use serde_json::Result;
use server_edge_datatypes as datatypes;
use server_edge_datatypes::gui::ServerRequests as GUIServerRequests;
use server_edge_datatypes::edge::ServerRequests as EdgeServerRequests;
use tokio::{sync::mpsc, time::interval};

use crate::{ServerHandle, WSSide};
use rand::{self, RngExt as _};

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

	let mut msg_stream = msg_stream
		.max_frame_size(20 * 1024 * 1024)
		.aggregate_continuations()
		.max_continuation_size(2 * 1024 * 1024);

	let (conn_tx, mut conn_rx) = mpsc::unbounded_channel();

	let (img_tx, mut img_rx) = mpsc::unbounded_channel();

	let send_img_tx= match side {
		WSSide::GUI => Some(img_tx),
		WSSide::Edge => None,
	};

	if side == WSSide::Edge {
		img_rx.close()
	}

	let conn_id = match side {
		WSSide::GUI => rand::rng().random::<ConnId>(),
		WSSide::Edge => {
			let Some(data) = initialise_edge_connection(&mut msg_stream).await else {
				let _ = session.close(Some(CloseReason{
					code: CloseCode::Protocol,
					description: Some(
						"Edge connections must send an initialisation command before any other data"
							.to_string()
						)
					}
				));
				return
			};
			data
		}
	};

	if !server.connect(conn_tx, conn_id, side, send_img_tx).await {
		eprintln!("Invalid edge ID");
		let _ = session.close(Some(CloseReason{
			code: CloseCode::Invalid,
			description: Some(
				"Invalid ID for initialisation: ID already in use"
					.to_string()
			)
		}
		));
		return;
	}

	match side {
		WSSide::GUI => {
			println!("GUI connected: {conn_id}");
		},

		WSSide::Edge => {
			println!("Edge connected: {conn_id}");
		},
	}

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

async fn initialise_edge_connection(msg_stream: &mut actix_ws::AggregatedMessageStream) -> Option<ConnId> {
	let Some(Ok(AggregatedMessage::Text(text))) = msg_stream.next().await else {
		eprintln!("Websocket stream received invalid input while waiting for initialisation");
		return None
	};

	let msg = text.trim();
	let data_unchecked = serde_json::from_str::<datatypes::edge::ServerRequests>(&msg);
	let Ok(command) = data_unchecked else {
		eprintln!("Websocket stream received invalid text input while waiting for initialisation");
		return None
	};

	let datatypes::edge::ServerRequests::InitialiseConnection(data) = command else {
		eprintln!("Websocket stream received invalid command while waiting for initialisation");
		return None
	};
	Some(data.edge_id)
}

async fn process_ws(
	server: &ServerHandle,
	text: &str,
	conn: ConnId,
	side: WSSide,
) {
	match side {
		WSSide::Edge => {
			edge_process(server, text, conn).await;
		},
		WSSide::GUI => {
			gui_process(server, text, conn).await;
		},
	}
}

async fn gui_process(server: &ServerHandle, text: &str, conn: ConnId) {
	let msg = text.trim();
    if msg == "\"Ping\"" || msg == "Ping" {
		return;
	}

	println!("Received from GUI: {msg}");

	let data_unchecked: Result<GUIServerRequests> =
		serde_json::from_str(msg);

	let Ok(data) = data_unchecked else {
		eprintln!(
			"Message could not deserialized due to: {:?}",
			data_unchecked.err().unwrap().to_string()
		);
		return;
	};

	match data {
		GUIServerRequests::ListEdges => server.list_edges(conn).await,

		GUIServerRequests::ListCameras => server.list_cameras(conn).await,

		GUIServerRequests::ListDisconnectedEdges => server.list_disconnected_edges(conn).await,

		GUIServerRequests::ForgetEdge(data) => {
			server.forget_edge(data).await
		}

		GUIServerRequests::ViewCameraConfig(data) => {
			server.view_camera_config(conn, data).await
		},

		GUIServerRequests::Configuration(data) => {
			server.configuration(conn, data, WSSide::GUI).await
		},

		GUIServerRequests::SetEdgeName(data) => server.set_edge_name(data).await,

		GUIServerRequests::SetEdgeProcessingType(data) => {
			server.set_edge_processing_type(data).await
		},

		GUIServerRequests::SetCameraThresholds(data) => {
			server.set_camera_thresholds(data).await
		},

		GUIServerRequests::SetDefaultThreshold(data) => {
			server.set_default_thresholds(data).await
		},

		GUIServerRequests::GetDefaultThresholds => {
			server.get_default_thresholds(conn).await
		},

		GUIServerRequests::SetNameCamera(data) => {
			server.set_name_camera(data).await
		},

		GUIServerRequests::StartViewingCamera(data) => {
			server.start_viewing_camera(conn, data).await
		},

		GUIServerRequests::StopViewingCamera => {
			server.stop_viewing_camera(conn).await
		},

		GUIServerRequests::Setpoint(data) => server.setpoint(Some(data)).await,

		GUIServerRequests::ForgetCameraRequest(data) => {
			server.forget_camera(conn, data, WSSide::GUI).await
		},

		GUIServerRequests::ForceRetryCamera(data) => {
			server.force_retry_camera(data).await
		},

		GUIServerRequests::ListProcessingTypes => {
			server.list_processing_types(conn).await
		},

		_ => {
			eprintln!("Message not recognised");
		},
	}
}

async fn edge_process(server: &ServerHandle, text: &str, conn: ConnId,) {
	let msg = text.trim();

	println!("Received from Edge: {msg}");

	let data_unchecked: Result<EdgeServerRequests> =
		serde_json::from_str(msg);

	let Ok(data) = data_unchecked else {
		eprintln!(
			"Message could not deserialized due to: {:?}",
			data_unchecked.err().unwrap().to_string()
		);
		return;
	};

	match data {
		EdgeServerRequests::InitialiseConnection(_) => {
			eprintln!(
				"Already initialised connection tried initialising: {:?}",
				conn
			);
		},

		EdgeServerRequests::RegisterCameras(data) => {
			server.register_cameras(conn, data.cameras).await
		},

		EdgeServerRequests::UpdateCameraStatus(data) => {
			server.update_camera_status(conn, data).await
		},

		EdgeServerRequests::ImageAnalysis(data) => {
			server.image_analysis(conn, data).await
		},

		EdgeServerRequests::Configuration(data) => {
			server.configuration(conn, data, WSSide::Edge).await
		},

		EdgeServerRequests::ForgetCameraRequest(data) => {
			server.forget_camera(conn, data, WSSide::Edge).await
		},

		EdgeServerRequests::Alert(data) => {
			server.alert_from_edge(conn, data).await
		},

		EdgeServerRequests::SetNameCamera(data) => {
			server.edge_camera_name_update(conn, data).await
		}

		_ => {
			eprintln!("Message not recognised");
		},
	}
}
