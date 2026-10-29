use actix_web::web::Bytes;
use actix_ws::{AggregatedMessage, CloseCode, CloseReason};
use api::{
	edge::ServerRequests as EdgeServerRequests,
	gui::ServerRequests as GUIServerRequests, shared::ConnId,
};
use futures_util::StreamExt as _;
use serde_json::Result;
use std::time::{Duration, Instant};
use tokio::{sync::mpsc, time::interval};

use crate::error::{ServerError, ServerResult};
use crate::logger::logger;
use crate::{Msg, WSSide, server::server_handle::ServerHandle};
use rand::{self, RngExt as _};
use tokio::time::Interval;

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(6);
const CLIENT_TIMEOUT: Duration = Duration::from_secs(12);

/// generic websocket handler - can process both edge and GUI websockets,
pub async fn ws_handler(
	server: ServerHandle,
	mut session: actix_ws::Session,
	msg_stream: actix_ws::MessageStream,
	side: WSSide,
) {
	let mut last_heartbeat = Instant::now();
	let interval = interval(HEARTBEAT_INTERVAL);

	let mut msg_stream = msg_stream
		.max_frame_size(20 * 1024 * 1024)
		.aggregate_continuations()
		.max_continuation_size(2 * 1024 * 1024);

	let (conn_tx, conn_rx) = mpsc::unbounded_channel();

	let (img_tx, mut img_rx) = mpsc::unbounded_channel();

	let send_img_tx = match side {
		WSSide::GUI => Some(img_tx),
		WSSide::Edge => None,
	};

	if side == WSSide::Edge {
		img_rx.close()
	}

	let conn_id = match side {
		WSSide::GUI => rand::rng().random::<ConnId>(),
		WSSide::Edge => {
			let Some(data) = initialise_edge_connection(&mut msg_stream).await
			else {
				let _ = session.close(Some(CloseReason{
					code: CloseCode::Protocol,
					description: Some(
						"Edge connections must send an initialisation command before any other data"
							.to_string()
						)
					}
				));
				return;
			};
			data
		},
	};

	if !server.connect(conn_tx, conn_id, side, send_img_tx).await {
		logger().log_warn("Edge ID already in use");
		let _ = session.close(Some(CloseReason {
			code: CloseCode::Invalid,
			description: Some(
				"Invalid ID for initialisation: ID already in use".to_string(),
			),
		}));
		return;
	}

	match side {
		WSSide::GUI => {
			logger().log_info(format!("GUI connected: {conn_id}").as_str());
		},

		WSSide::Edge => {
			logger().log_info(format!("Edge connected: {conn_id}").as_str());
		},
	}

	let close_reason: Option<CloseReason> = handle_loop(
		&mut session,
		&mut msg_stream,
		&mut last_heartbeat,
		side,
		&server,
		conn_id,
		conn_rx,
		img_rx,
		interval,
	)
	.await;

	match side {
		WSSide::GUI => {
			logger().log_info(format!("GUI disconnected: {conn_id}").as_str());
		},

		WSSide::Edge => {
			logger().log_info(format!("Edge disconnected: {conn_id}").as_str());
		},
	}

	server.disconnect(conn_id, side).await;

	let _ = session.close(close_reason).await;
}

async fn handle_loop(
	session: &mut actix_ws::Session,
	msg_stream: &mut actix_ws::AggregatedMessageStream,
	last_heartbeat: &mut Instant,
	side: WSSide,
	server: &ServerHandle,
	conn: ConnId,
	mut conn_rx: mpsc::UnboundedReceiver<Msg>,
	mut img_rx: mpsc::UnboundedReceiver<Bytes>,
	mut interval: Interval,
) -> Option<CloseReason> {
	loop {
		tokio::select! { //waits for the first of the following events:
			//handles incoming ws traffic
			Some(Ok(msg)) = msg_stream.next() => {
				if let Err(err) = process_ws_request(
					msg,
					session,
					last_heartbeat,
					side,
					&server,
					conn,
				).await {
					match err {
						ServerError::CloseConnection{ reason } => {
							break reason;
						}

						_ => {}
					}
				}
			}

			//Sends any text input to the conn_tx sender back to the client
			Some(chat_msg) = conn_rx.recv() => {
				if let Err(err) = send_text(
					session,
					&*chat_msg,
					side,
				).await {
					match err {
						ServerError::CloseConnection{ reason } => {
							break reason;
						}

						_ => {}
					}
				}
			}

			Some(img_data) = img_rx.recv() => {
				let closed = session.binary(img_data).await;
				if closed.is_err() {
					break None;
				}
			}

			//keeps track of timeout conditions
			_ = interval.tick() => {
				if let Err(err) = handle_tick(
					session,
					last_heartbeat,
					side,
					conn
				).await {
					match err {
						ServerError::CloseConnection{ reason } => {
							break reason;
						}
						_ => {}
					}
				}
			}

			else => {
				break None;
			}
		}
	}
}

async fn process_ws_request(
	msg: AggregatedMessage,
	session: &mut actix_ws::Session,
	last_heartbeat: &mut Instant,
	side: WSSide,
	server: &ServerHandle,
	conn: ConnId,
) -> ServerResult<()> {
	match msg {
		AggregatedMessage::Ping(bytes) => {
			*last_heartbeat = Instant::now();

			//Not checking if the session is closed here because the chance of a ping being sent then the connection immediately closing is vanishingly unlikely
			let _ = session.pong(&bytes).await;
		},

		AggregatedMessage::Pong(_) => {
			*last_heartbeat = Instant::now();
		},

		AggregatedMessage::Text(text) => {
			*last_heartbeat = Instant::now();
			process_ws(&server, &text, conn, side).await;
		},

		AggregatedMessage::Binary(bin) => match side {
			WSSide::GUI => {
				return Err(ServerError::CloseConnection { reason: None });
			},

			WSSide::Edge => {
				server.route_image(conn, bin).await;
			},
		},
		AggregatedMessage::Close(reason) => {
			return Err(ServerError::CloseConnection { reason });
		},
	}

	Ok(())
}

async fn send_text(
	session: &mut actix_ws::Session,
	chat_msg: &str,
	side: WSSide,
) -> ServerResult<()> {
	match side {
		WSSide::GUI => {
			#[cfg(debug_assertions)]
			println!("Sent to GUI: {chat_msg}");
		},

		WSSide::Edge => {
			#[cfg(debug_assertions)]
			println!("Sent to Edge: {chat_msg}");
		},
	}
	let closed = session.text(chat_msg).await;
	if closed.is_err() {
		return Err(ServerError::CloseConnection { reason: None });
	}

	Ok(())
}

async fn handle_tick(
	session: &mut actix_ws::Session,
	last_heartbeat: &mut Instant,
	side: WSSide,
	conn: ConnId,
) -> ServerResult<()> {
	if Instant::now().duration_since(*last_heartbeat) > CLIENT_TIMEOUT {
		match side {
			WSSide::GUI => {
				logger().log_info(format!("GUI timed out: {conn}").as_str());
			},

			WSSide::Edge => {
				logger().log_info(format!("Edge timed out: {conn}").as_str());
			},
		}
		return Err(ServerError::CloseConnection { reason: None });
	}
	let closed = session.ping(b"").await;
	if closed.is_err() {
		return Err(ServerError::CloseConnection { reason: None });
	}

	Ok(())
}

async fn initialise_edge_connection(
	msg_stream: &mut actix_ws::AggregatedMessageStream,
) -> Option<ConnId> {
	let Some(Ok(AggregatedMessage::Text(text))) = msg_stream.next().await
	else {
		logger().log_warn(
			"Websocket stream received invalid input while waiting for initialisation",
		);
		return None;
	};

	let msg = text.trim();
	let data_unchecked =
		serde_json::from_str::<api::edge::ServerRequests>(&msg);
	let Ok(command) = data_unchecked else {
		logger().log_warn(
			"Websocket stream received invalid text input while waiting for initialisation"
		);
		return None;
	};

	let api::edge::ServerRequests::InitialiseConnection(data) = command else {
		logger().log_warn(
			"Websocket stream received invalid command while waiting for initialisation",
		);
		return None;
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

	#[cfg(debug_assertions)]
	println!("Received from GUI: {msg}");

	let data_unchecked: Result<GUIServerRequests> = serde_json::from_str(msg);

	let data;
	match data_unchecked {
		Ok(value) => {
			data = value;
		},

		Err(e) => {
			logger().log_warn(
				format!(
					"Message could not deserialized due to: {:?}",
					e.to_string()
				)
				.as_str(),
			);
			return;
		},
	}

	match data {
		GUIServerRequests::ListEdges => server.list_edges(conn).await,

		GUIServerRequests::ListCameras => server.list_cameras(conn).await,

		GUIServerRequests::ListDisconnectedEdges => {
			server.list_disconnected_edges(conn).await
		},

		GUIServerRequests::ForgetEdge(data) => server.forget_edge(data).await,

		GUIServerRequests::ViewCameraConfig(data) => {
			server.view_camera_config(conn, data).await
		},

		GUIServerRequests::Configuration(data) => {
			server.configuration(conn, data, WSSide::GUI).await
		},

		GUIServerRequests::SetEdgeName(data) => {
			server.set_edge_name(data).await
		},

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

		GUIServerRequests::IsCameraCalibrated(data) => {
			server.camera_calibrated(conn, data).await;
		},

		_ => {
			logger().log_warn("Received unrecognized GUI command");
		},
	}
}

async fn edge_process(server: &ServerHandle, text: &str, conn: ConnId) {
	let msg = text.trim();

	#[cfg(debug_assertions)]
	println!("Received from Edge: {msg}");

	let data_unchecked: Result<EdgeServerRequests> = serde_json::from_str(msg);

	let data;
	match data_unchecked {
		Ok(value) => {
			data = value;
		},

		Err(e) => {
			logger().log_warn(
				format!(
					"Message could not deserialized due to: {:?}",
					e.to_string()
				)
				.as_str(),
			);
			return;
		},
	}

	match data {
		EdgeServerRequests::InitialiseConnection(_) => {},

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
		},

		EdgeServerRequests::CameraCalibrationUpdate(data) => {
			server.camera_calibration_update(conn, data).await
		},

		EdgeServerRequests::CameraCalibrationStatus(data) => {
			server.camera_calibration_status(conn, data).await
		},

		_ => {
			logger().log_warn("Received unrecognized Edge command");
		},
	}
}
