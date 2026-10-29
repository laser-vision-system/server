//!
pub mod commands;
pub mod server_handle;

use crate::WSSide::Edge;
use crate::error::{ServerError, ServerResult};
use crate::file_manager::FileManagerHandle;
use crate::logger::logger;
use crate::server::commands::Command;
use crate::server::server_handle::ServerHandle;
use crate::server_persist_data::{PersistentServerData, ShortCameraDetails};
use crate::{Msg, WSSide};
use actix_web::web::Bytes;
use api::{
	edge::{self, EdgeRequests},
	gui::{self, CameraDetails, GUIRequests},
	shared::{self, CameraStatus, Deviation},
	shared::{CameraId, ConnId},
};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use std::{
	collections::{HashMap, HashSet},
	io,
};
use tokio::sync::{mpsc, mpsc::UnboundedSender};
use tokio::time::interval;

#[derive(Hash, Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct UniqueCameraID {
	pub conn: ConnId,
	pub camera: CameraId,
}

impl From<String> for UniqueCameraID {
	fn from(id: String) -> Self {
		serde_json::from_str(&*id)
			.unwrap_or(UniqueCameraID { conn: 0, camera: 0 })
	}
}

impl From<UniqueCameraID> for (ConnId, CameraId) {
	fn from(id: UniqueCameraID) -> (ConnId, CameraId) {
		let UniqueCameraID { conn, camera } = id;
		(conn, camera)
	}
}

#[derive(Debug)]
pub struct Server {
	gui_sessions: HashMap<ConnId, UnboundedSender<Msg>>,

	gui_img_route: HashMap<ConnId, UnboundedSender<Bytes>>,

	edge_sessions: HashMap<ConnId, UnboundedSender<Msg>>,

	edge_names: HashMap<ConnId, String>,

	cameras: HashMap<UniqueCameraID, CameraDetails>,

	video_routes: HashMap<ConnId, (ConnId, CameraId)>,

	default_warn_threshold: Deviation,

	default_alert_threshold: Deviation,

	default_warn_above_threshold: Deviation,

	default_alert_above_threshold: Deviation,

	cmd_rx: mpsc::UnboundedReceiver<Command>,

	file_manager: FileManagerHandle,
}

impl Server {
	pub async fn new(file_manager: FileManagerHandle) -> (Self, ServerHandle) {
		let (tx, rx) = mpsc::unbounded_channel();

		let loaded_state = file_manager.load_server_state().await;

		let mut cameras: HashMap<UniqueCameraID, CameraDetails> =
			HashMap::new();
		for camera in loaded_state.cameras {
			cameras.insert(
				UniqueCameraID::from(camera.0),
				CameraDetails::from(camera.1),
			);
		}

		(
			Self {
				gui_sessions: HashMap::new(),
				gui_img_route: HashMap::new(),
				edge_sessions: HashMap::new(),
				edge_names: loaded_state.edge_names,
				cameras,
				video_routes: HashMap::new(),
				default_warn_threshold: loaded_state.default_warn_threshold,
				default_alert_threshold: loaded_state.default_alert_threshold,
				default_warn_above_threshold: loaded_state
					.default_warn_above_threshold,
				default_alert_above_threshold: loaded_state
					.default_alert_above_threshold,
				cmd_rx: rx,
				file_manager,
			},
			ServerHandle::new(tx),
		)
	}

	async fn connect(
		&mut self,
		tx: UnboundedSender<Msg>,
		img_tx: Option<UnboundedSender<Bytes>>,
		conn_id: ConnId,
		side: WSSide,
	) -> bool {
		match side {
			WSSide::GUI => {
				self.gui_sessions.insert(conn_id, tx);
				self.gui_img_route.insert(
					conn_id,
					img_tx
						.expect("GUI handler tried to register without img_tx"),
				);
			},
			WSSide::Edge => {
				if self.edge_sessions.contains_key(&conn_id) {
					return false;
				}

				self.edge_sessions.insert(conn_id, tx);
				if !self.edge_names.contains_key(&conn_id) {
					self.edge_names.insert(
						conn_id,
						format!("Edge {}", self.edge_names.len()),
					);
				}

				for gui in self.gui_sessions.clone().keys() {
					self.list_edges(*gui).await;
				}
			},
		}
		true
	}

	async fn disconnect(&mut self, conn_id: ConnId, side: WSSide) {
		match side {
			WSSide::Edge => {
				self.edge_sessions.remove(&conn_id);

				let mut cameras: HashSet<(ConnId, CameraId)> = HashSet::new();

				for camera in self.cameras.keys() {
					if camera.conn == conn_id {
						cameras.insert(<(ConnId, CameraId)>::from(*camera));
					}
				}

				for camera in cameras {
					let new_status = edge::UpdateCameraStatus {
						camera_id: camera.1,
						status: CameraStatus::Disconnected,
					};
					self.update_camera_status(camera.0, new_status).await;
				}
			},
			WSSide::GUI => {
				self.stop_viewing_camera(conn_id).await;
				self.gui_sessions.remove(&conn_id);
				self.gui_img_route.remove(&conn_id);
			},
		}
	}

	fn get_edge_name(&mut self, conn_id: ConnId) -> ServerResult<String> {
		self.edge_names
			.get(&conn_id)
			.cloned()
			.ok_or_else(|| ServerError::EdgeIdNotFound)
	}

	fn is_connected(&self, conn_id: ConnId, camera: CameraId) -> bool {
		let camera_id = UniqueCameraID {
			conn: conn_id,
			camera,
		};

		match self.cameras.get(&camera_id) {
			Some(result) => match result.state {
				CameraStatus::Active => true,

				_ => false,
			},

			None => false,
		}
	}

	fn camera_in_use(&self, conn_id: ConnId, camera: CameraId) -> bool {
		let camera_id = &(conn_id, camera);
		for route in self.video_routes.values() {
			if route == camera_id {
				return true;
			}
		}

		false
	}

	async fn register_cameras(
		&mut self,
		conn: ConnId,
		cameras: HashMap<CameraId, edge::NewCameraInfo>,
	) {
		let mut name_updates: HashMap<CameraId, String> = HashMap::new();
		for camera in cameras {
			let camera_id = camera.0;

			let mut warn_below_threshold = self.default_warn_threshold;
			let mut alert_below_threshold = self.default_alert_threshold;
			let mut alert_above_threshold = self.default_alert_above_threshold;
			let mut warn_above_threshold = self.default_warn_above_threshold;
			let mut updated_name: bool = false;

			if self.cameras.contains_key(&UniqueCameraID {
				conn,
				camera: camera_id,
			}) {
				let old_details = &self.cameras[&UniqueCameraID {
					conn,
					camera: camera_id,
				}];
				if old_details.name != camera.1.name {
					name_updates.insert(camera_id, old_details.name.clone());
				}
				warn_below_threshold = old_details.warn_below_threshold;
				alert_below_threshold = old_details.alert_below_threshold;
				alert_above_threshold = old_details.alert_above_threshold;
				warn_above_threshold = old_details.warn_above_threshold;
			}

			let camera_details = CameraDetails {
				name: camera.1.name,
				state: camera.1.status,
				warn_below_threshold,
				alert_below_threshold,
				warn_above_threshold,
				alert_above_threshold,
			};
			self.cameras.insert(
				UniqueCameraID {
					conn,
					camera: camera_id,
				},
				camera_details,
			);
		}

		for (camera, name) in name_updates {
			self.set_name_camera(conn, camera, name).await;
		}

		for gui in self.gui_sessions.clone().keys() {
			self.list_cameras(*gui).await;
		}
	}

	async fn update_camera_status(
		&mut self,
		conn: ConnId,
		data: edge::UpdateCameraStatus,
	) {
		let get_details = self.cameras.get(&UniqueCameraID {
			conn,
			camera: data.camera_id,
		});
		let Some(details) = get_details else {
			logger().log_warn(
				format!(
					"Received UpdateCameraStatus command for unknown camera: ({conn}, {})",
					data.camera_id
				)
				.as_str(),
			);
			return;
		};

		let new_details = CameraDetails {
			name: details.name.clone(),
			state: data.status,
			warn_below_threshold: details.warn_below_threshold,
			alert_below_threshold: details.alert_below_threshold,
			warn_above_threshold: details.warn_above_threshold,
			alert_above_threshold: details.alert_above_threshold,
		};
		self.cameras.insert(
			UniqueCameraID {
				conn,
				camera: data.camera_id,
			},
			new_details,
		);
		match data.status {
			CameraStatus::Active => {
				for gui in self.gui_sessions.clone().keys() {
					self.list_cameras(*gui).await;
				}
			},

			_ => self.disconnect_camera(conn, data.camera_id).await,
		}
	}

	async fn disconnect_camera(&mut self, conn: ConnId, camera: CameraId) {
		for gui in self.gui_sessions.clone().keys() {
			self.list_cameras(*gui).await;
		}

		self.video_routes
			.retain(|_, record| !(record.0 == conn && record.1 == camera));
	}

	async fn image_analysis(
		&mut self,
		conn: ConnId,
		analysis: edge::ImageAnalysis,
	) {
		let gui_struct = gui::ImageAnalysis {
			conn_id: conn.to_string(),
			camera_id: analysis.camera_id,
			deviation: analysis.deviation,
		};
		let msg = GUIRequests::ImageAnalysis(gui_struct);
		let send = serde_json::to_string(&msg)
			.expect("Could not serialize GUI request");
		for gui in self.gui_sessions.values() {
			let _ = gui.send(send.clone());
		}
	}

	async fn set_point(&mut self, target: Option<gui::Setpoint>) {
		if let Some(target) = target {
			if self.is_connected(target.conn_id, target.camera_id) {
				self.send_set_point(target.conn_id, target.camera_id).await;
			}
		} else {
			let mut targets: HashSet<(ConnId, CameraId)> = HashSet::new();

			for target in self.cameras.keys() {
				targets.insert(<(ConnId, CameraId)>::from(*target));
			}

			for target in targets {
				self.send_set_point(target.0, target.1).await;
			}
		}
	}

	async fn send_set_point(&mut self, conn: ConnId, camera: CameraId) {
		let edge_struct = edge::Setpoint { camera_id: camera };
		let msg = EdgeRequests::Setpoint(edge_struct);
		let send = serde_json::to_string(&msg)
			.expect("Could not serialize Edge request");
		if let Some(target) = &self.edge_sessions.get(&conn) {
			let _ = target.send(send.clone());
		}
	}

	async fn list_cameras(&mut self, conn: ConnId) {
		let mut cameras_sendable: HashMap<String, CameraDetails> =
			HashMap::new();

		for camera in self.cameras.clone() {
			let key = format!("{:?}", camera.0);
			cameras_sendable.insert(key, camera.1);
		}

		let gui_struct = gui::ListOfCameras {
			cameras: cameras_sendable,
		};

		let msg = GUIRequests::ListOfCameras(gui_struct);
		let send = serde_json::to_string(&msg)
			.expect("Could not serialize GUI request");

		let target = self.gui_sessions.get(&conn);

		if let Some(target) = target {
			let _ = target.send(send.clone());
		}
	}

	async fn set_name_camera(
		&mut self,
		conn_id: ConnId,
		camera: CameraId,
		name: String,
	) {
		if !self.is_connected(conn_id, camera) {
			return;
		}

		let edge_struct = edge::SetNameCamera {
			camera_id: camera,
			name,
		};

		let msg = EdgeRequests::SetNameCamera(edge_struct);
		let send = serde_json::to_string(&msg)
			.expect("Could not serialize Edge request");

		if let Some(target) = &self.edge_sessions.get(&conn_id) {
			let _ = target.send(send.clone());
		}
	}

	async fn edge_camera_name_update(
		&mut self,
		conn_id: ConnId,
		data: edge::SetNameCamera,
	) {
		let camera_id = UniqueCameraID {
			conn: conn_id,
			camera: data.camera_id,
		};
		let old_camera_data;

		if let Some(camera_data) = self.cameras.get_mut(&camera_id) {
			logger().log_warn(format!(
				"Received CameraNameUpdate command from edge for unknown camera: ({conn_id}, {})",
				data.camera_id
			).as_str());

			old_camera_data = camera_data
		} else {
			return;
		}

		let new_camera_data = CameraDetails {
			name: data.name,
			state: old_camera_data.state,
			warn_below_threshold: old_camera_data.warn_below_threshold,
			warn_above_threshold: old_camera_data.warn_above_threshold,
			alert_above_threshold: old_camera_data.alert_above_threshold,
			alert_below_threshold: old_camera_data.alert_below_threshold,
		};

		self.cameras.insert(camera_id, new_camera_data);

		for gui in self.gui_sessions.clone().keys() {
			self.list_cameras(*gui).await;
		}
	}

	async fn start_viewing_camera(
		&mut self,
		gui_conn: ConnId,
		edge_conn: ConnId,
		camera: CameraId,
	) {
		if !self.is_connected(edge_conn, camera) {
			return;
		}
		if self.video_routes.contains_key(&gui_conn) {
			if self.video_routes[&gui_conn] == (edge_conn, camera) {
				return;
			}
			self.stop_viewing_camera(gui_conn).await;
		}

		let mut skip_camera_startup = false;

		if self.camera_in_use(edge_conn, camera) {
			skip_camera_startup = true;
		}

		self.video_routes.insert(gui_conn, (edge_conn, camera));

		if skip_camera_startup {
			return;
		}

		let edge_struct = edge::ToggleViewingCamera {
			camera_id: camera,
			start_viewing: true,
		};

		let msg = EdgeRequests::ToggleViewingCamera(edge_struct);
		let send = serde_json::to_string(&msg)
			.expect("Could not serialize Edge request");

		if let Some(target) = &self.edge_sessions.get(&edge_conn) {
			let _ = target.send(send.clone());
		}
	}

	async fn stop_viewing_camera(&mut self, conn: ConnId) {
		let c_video_routes = self.video_routes.clone();
		let mut c2_video_routes = self.video_routes.clone();

		let route;
		if let Some(entry) = c_video_routes.get(&conn) {
			route = entry;
		} else {
			return;
		}

		c2_video_routes.remove(&conn);
		self.video_routes = c2_video_routes;

		if self.camera_in_use(route.0, route.1) {
			return;
		}

		let edge_struct = edge::ToggleViewingCamera {
			camera_id: route.1,
			start_viewing: false,
		};
		let msg = EdgeRequests::ToggleViewingCamera(edge_struct);
		let send = serde_json::to_string(&msg)
			.expect("Could not serialize Edge request");

		if let Some(target) = &self.edge_sessions.get(&route.0) {
			let _ = target.send(send.clone());
		}
	}

	async fn configuration(
		&mut self,
		conn: ConnId,
		config: shared::Configuration,
		side: WSSide,
	) {
		match side {
			WSSide::GUI => {
				if !self.is_connected(config.conn_id, config.camera_id) {
					return;
				}
			},
			Edge => {
				if !self.is_connected(conn, config.camera_id) {
					return;
				}
			},
		}

		let send: String;
		let target: Option<&UnboundedSender<Msg>>;
		match side {
			WSSide::GUI => {
				let fwd_config = shared::Configuration {
					conn_id: conn,
					camera_id: config.camera_id,
					configuration: config.configuration,
				};
				let msg = EdgeRequests::Configuration(fwd_config);
				send = serde_json::to_string(&msg)
					.expect("Could not serialize Edge request");
				target = self.edge_sessions.get(&config.conn_id);
			},
			WSSide::Edge => {
				let fwd_config = gui::Configuration {
					conn_id: conn.to_string(),
					camera_id: config.camera_id,
					configuration: config.configuration,
				};
				let msg = GUIRequests::Configuration(fwd_config);
				send = serde_json::to_string(&msg)
					.expect("Could not serialize GUI request");

				target = self.gui_sessions.get(&config.conn_id);
			},
		}
		let Some(target) = target else {
			return;
		};
		let _ = target.send(send.clone());
	}

	async fn view_camera_config(
		&mut self,
		conn: ConnId,
		camera: shared::ViewCameraConfig,
	) {
		if !self.is_connected(camera.conn_id, camera.camera_id) {
			return;
		}

		let fwd_data: shared::ViewCameraConfig = shared::ViewCameraConfig {
			conn_id: conn,
			camera_id: camera.camera_id,
		};

		let msg = EdgeRequests::ViewCameraConfig(fwd_data);
		let send = serde_json::to_string(&msg)
			.expect("Could not serialize Edge request");

		if let Some(target) = &self.edge_sessions.get(&camera.conn_id) {
			let _ = target.send(send.clone());
		}
	}

	async fn forget_camera(
		&mut self,
		conn: ConnId,
		target: shared::ForgetCameraRequest,
		side: WSSide,
	) {
		let camera: UniqueCameraID;
		match side {
			WSSide::GUI => {
				camera = UniqueCameraID {
					conn: target.conn_id,
					camera: target.camera_id,
				};
			},

			WSSide::Edge => {
				camera = UniqueCameraID {
					conn,
					camera: target.camera_id,
				};
				if !self.gui_sessions.contains_key(&target.conn_id)
					&& !target.confirm
				{
					return;
				}
			},
		}
		if !self.cameras.contains_key(&camera) {
			return;
		}

		match self.cameras.get(&camera) {
			None => {},
			Some(camera_details) => match camera_details.state {
				CameraStatus::Active => return,

				_ => {},
			},
		}

		match side {
			WSSide::Edge => {
				let data = gui::ForgetCamera {
					conn_id: conn.to_string(),
					camera_id: target.camera_id,
					confirm: target.confirm,
				};
				let msg = GUIRequests::ForgetCamera(data);
				let send = serde_json::to_string(&msg)
					.expect("Could not serialize GUI request");

				let Some(gui) = self.gui_sessions.get(&target.conn_id) else {
					return;
				};

				let _ = gui.send(send.clone());

				if target.confirm {
					self.cameras.remove(&camera);
					for gui in self.gui_sessions.clone().keys() {
						self.list_cameras(*gui).await;
					}
				}
			},

			WSSide::GUI => {
				let data = shared::ForgetCameraRequest {
					conn_id: conn,
					camera_id: target.camera_id,
					confirm: target.confirm,
				};
				let msg = EdgeRequests::ForgetCameraRequest(data);
				let send = serde_json::to_string(&msg)
					.expect("Could not serialize Edge request");

				if let Some(target) = self.gui_sessions.get(&target.conn_id) {
					let _ = target.send(send.clone());
				}
			},
		}
	}

	async fn route_image(&mut self, conn_id: ConnId, bytes: Bytes) {
		let len = bytes.len();
		if len < 10 {
			return;
		}

		let mut buf = [0_u8; 8];
		buf[..8].copy_from_slice(&bytes[..8]);
		let camera_id: CameraId = CameraId::from_be_bytes(buf);

		if !self.camera_in_use(conn_id, camera_id) {
			// If the edge is sending images for cameras not being viewed, tell it to stop.
			if self.cameras.contains_key(&UniqueCameraID {
				conn: conn_id,
				camera: camera_id,
			}) {
				if let Some(edge) = self.edge_sessions.get(&conn_id) {
					let send = edge::ToggleViewingCamera {
						camera_id,
						start_viewing: false,
					};
					let msg = serde_json::to_string(
						&EdgeRequests::ToggleViewingCamera(send),
					)
					.expect("Could not serialize Edge request");
					let _ = edge.send(msg);
				}
			}
			logger().log_warn(
				format!(
					"Image sent from unused camera: ({conn_id}, {})",
					camera_id
				)
				.as_str(),
			);
			return;
		}

		let img = Bytes::copy_from_slice(&bytes[8..]);

		if img.is_empty() {
			return;
		}

		for gui in self.video_routes.clone() {
			if gui.1.0 == conn_id && gui.1.1 == camera_id {
				if let Some(target) = self.gui_img_route.get(&gui.0) {
					let _ = target.send(img.clone());
				}
			}
		}
	}

	async fn set_camera_thresholds(&mut self, data: gui::SetCameraThresholds) {
		let camera = UniqueCameraID {
			conn: data.edge_id,
			camera: data.camera_id,
		};
		if self.cameras.contains_key(&camera) {
			let old_details = &self.cameras[&camera];
			let new_details = CameraDetails {
				name: old_details.name.clone(),
				state: old_details.state,
				warn_below_threshold: data.warn_below_threshold,
				alert_below_threshold: data.alert_below_threshold,
				warn_above_threshold: data.warn_above_threshold,
				alert_above_threshold: data.alert_above_threshold,
			};
			self.cameras.insert(camera, new_details);

			for gui in self.gui_sessions.clone().keys() {
				self.list_cameras(*gui).await;
			}
		}
	}

	async fn set_default_thresholds(&mut self, data: gui::SetDefaultThreshold) {
		self.default_warn_threshold = data.warn_below_threshold;
		self.default_alert_threshold = data.alert_below_threshold;
		self.default_warn_above_threshold = data.warn_above_threshold;
		self.default_alert_above_threshold = data.alert_above_threshold;
	}

	async fn set_edge_processing(&mut self, data: gui::SetEdgeProcessingType) {
		let send_data = edge::SetProcessingType {
			processing_type: data.processing_type,
		};
		let msg = EdgeRequests::SetProcessingType(send_data);

		let send = &serde_json::to_string(&msg)
			.expect("Could not serialize Edge request");

		let target = self.edge_sessions.get(&data.edge_id);

		if let Some(target) = target {
			let _ = target.send(send.clone());
		}
	}

	async fn get_default_thresholds(&mut self, conn_id: ConnId) {
		let data = gui::DefaultThresholds {
			warn_below_threshold: self.default_warn_threshold,
			alert_below_threshold: self.default_alert_threshold,
			warn_above_threshold: self.default_warn_above_threshold,
			alert_above_threshold: self.default_alert_above_threshold,
		};

		let msg = GUIRequests::DefaultThresholds(data);

		let send = &serde_json::to_string(&msg)
			.expect("Could not serialize GUI request");
		let target = self.gui_sessions.get(&conn_id);

		if let Some(target) = target {
			let _ = target.send(send.clone());
		}
	}

	async fn list_edges(&mut self, conn_id: ConnId) {
		let mut edge_names_copy = HashMap::new();

		let mut active_edges = HashSet::new();

		for conn in self.edge_sessions.keys() {
			active_edges.insert(conn);
		}

		for edge in self.edge_names.clone() {
			if !active_edges.contains(&edge.0) {
				continue;
			}
			edge_names_copy.insert(edge.0.to_string(), edge.1);
		}

		let data = gui::ListOfEdges {
			edges: edge_names_copy,
		};
		let msg = GUIRequests::ListOfEdges(data);
		let send = &serde_json::to_string(&msg)
			.expect("Could not serialize GUI request");

		let target = self.gui_sessions.get(&conn_id);
		if let Some(target) = target {
			let _ = target.send(send.clone());
		}
	}

	async fn disconnected_edges(&mut self, conn_id: ConnId) {
		let mut edge_names_copy = HashMap::new();

		let mut active_edges = HashSet::new();

		for conn in self.edge_sessions.keys() {
			active_edges.insert(conn);
		}

		for edge in self.edge_names.clone() {
			if active_edges.contains(&edge.0) {
				continue;
			}
			edge_names_copy.insert(edge.0.to_string(), edge.1);
		}

		let data = gui::DisconnectedEdges {
			edges: edge_names_copy,
		};
		let msg = GUIRequests::DisconnectedEdges(data);
		let send = &serde_json::to_string(&msg)
			.expect("Could not serialize GUI request");

		let target = self.gui_sessions.get(&conn_id);
		if let Some(target) = target {
			let _ = target.send(send.clone());
		}
	}

	async fn forget_edge(&mut self, data: gui::ForgetEdge) {
		if self.edge_sessions.contains_key(&data.conn) {
			return;
		}

		if !self.edge_names.contains_key(&data.conn) {
			return;
		}

		self.edge_names.remove(&data.conn);

		self.cameras.retain(|camera, _| camera.conn != data.conn);

		for gui in self.gui_sessions.clone().keys() {
			self.list_edges(*gui).await;
			self.disconnected_edges(*gui).await;
			self.list_cameras(*gui).await;
		}
	}

	async fn set_edge_name(&mut self, data: gui::SetEdgeName) {
		self.edge_names
			.entry(data.edge_id)
			.and_modify(|entry| *entry = data.name);
	}

	async fn send_alert(
		&mut self,
		conn_id: Option<ConnId>,
		alert: edge::Alert,
		target: Option<ConnId>,
	) {
		let data = gui::Alert {
			conn_id,
			camera_id: alert.camera_id,
			alert_code: alert.alert_code,
			alert_type: alert.alert_type,
			alert_name: alert.alert_name,
			alert_description: alert.alert_description,
		};
		let msg = GUIRequests::Alert(data);
		let send = serde_json::to_string(&msg)
			.expect("Could not serialize GUI request");

		if let Some(target) = target {
			if let Some(gui) = self.gui_sessions.get(&target) {
				let _ = gui.send(send.clone());
			}
		} else {
			for gui in self.gui_sessions.clone() {
				let _ = gui.1.send(send.clone());
			}
		}
	}

	async fn force_retry_camera(&mut self, data: gui::ForceRetryCamera) {
		let send_data = edge::ForceRetryCamera {
			camera_id: data.camera_id,
		};
		let msg = EdgeRequests::ForceRetryCamera(send_data);
		let send = &serde_json::to_string(&msg)
			.expect("Could not serialize Edge request");
		let target = self.gui_sessions.get(&data.camera_id);
		if let Some(target) = target {
			let _ = target.send(send.clone());
		}
	}

	async fn list_processing_types(&mut self, conn_id: ConnId) {
		let data = gui::ProcessingTypes::default();
		let msg = GUIRequests::ProcessingTypes(data);
		let send = &serde_json::to_string(&msg)
			.expect("Could not serialize GUI request");
		let target = self.gui_sessions.get(&conn_id);
		if let Some(target) = target {
			let _ = target.send(send.clone());
		}
	}

	async fn get_persist_data(&mut self) -> PersistentServerData {
		let mut cameras: HashMap<String, ShortCameraDetails> = HashMap::new();
		for camera in self.cameras.clone() {
			cameras.insert(
				String::from(camera.0),
				ShortCameraDetails::from(camera.1),
			);
		}

		PersistentServerData {
			edge_names: self.edge_names.clone(),
			cameras,
			default_alert_threshold: self.default_alert_threshold,
			default_alert_above_threshold: self.default_alert_above_threshold,
			default_warn_above_threshold: self.default_warn_above_threshold,
			default_warn_threshold: self.default_warn_threshold,
		}
	}

	async fn clear_disconnected_channels(&mut self) {
		for edge in self.edge_sessions.clone() {
			if edge.1.is_closed() {
				self.disconnect(edge.0, WSSide::Edge).await;
			}
		}

		for gui in self.gui_sessions.clone() {
			if gui.1.is_closed() {
				self.disconnect(gui.0, WSSide::GUI).await;
			}
		}
	}

	pub async fn run(mut self) -> io::Result<()> {
		let mut interval = interval(Duration::from_secs(15));
		interval.tick().await;
		loop {
			tokio::select! {
				Some(cmd) = self.cmd_rx.recv() => {
					match cmd {
						Command::Connect {
							conn_tx,
							res_tx,
							conn_id,
							side,
							img_tx,
						} => {
							let success = self.connect(conn_tx, img_tx, conn_id, side).await;
							let _ = res_tx.send(success);
						},

						Command::Disconnect { conn, side } => {
							self.disconnect(conn, side).await;
						},

						Command::GetEdgeName { res_tx, conn_id} => {
							let result = self.get_edge_name(conn_id);
							let _ = res_tx.send(result);
						}

						Command::RegisterCameras { conn, cameras } => {
							self.register_cameras(conn, cameras).await;
						},

						Command::UpdateCameraStatus { conn, data } => {
							self.update_camera_status(conn, data).await;
						},

						Command::ImageAnalysis { conn, analysis } => {
							self.image_analysis(conn, analysis).await;
						},

						Command::Setpoint { target } => {
							self.set_point(target).await;
						},

						Command::ListCameras { conn } => {
							self.list_cameras(conn).await;
						},

						Command::SetNameCamera { conn, camera, name } => {
							self.set_name_camera(conn, camera, name).await;
						},

						Command::EdgeCameraNameUpdate {conn, data} => {
							self.edge_camera_name_update(conn, data).await;
						}

						Command::StartViewingCamera {
							gui_conn,
							edge_conn,
							camera,
						} => {
							self.start_viewing_camera(gui_conn, edge_conn, camera)
								.await;
						},

						Command::StopViewingCamera { conn } => {
							self.stop_viewing_camera(conn).await;
						},

						Command::Configuration { conn, config, side } => {
							self.configuration(conn, config, side).await;
						},

						Command::ViewCameraConfig { conn, camera } => {
							self.view_camera_config(conn, camera).await;
						},

						Command::ForgetCamera { conn, target, side } => {
							self.forget_camera(conn, target, side).await;
						},

						Command::RouteImage { conn, bytes } => {
							self.route_image(conn, bytes).await;
						},

						Command::SetCameraThresholds { data } => {
							self.set_camera_thresholds(data).await;
						},

						Command::SetDefaultThresholds { data } => {
							self.set_default_thresholds(data).await;
						},

						Command::SetEdgeProcessingType { data } => {
							self.set_edge_processing(data).await;
						},

						Command::GetDefaultThresholds { conn } => {
							self.get_default_thresholds(conn).await;
						},

						Command::ListEdges { conn } => {
							self.list_edges(conn).await;
						},

						Command::ListDisconnectedEdges { conn } => {
							self.disconnected_edges(conn).await;
						}

						Command::ForgetEdge { data } => {
							self.forget_edge(data).await;
						}

						Command::SetEdgeName { data } => {
							self.set_edge_name(data).await;
						},

						Command::AlertFromEdge { conn, data } => {
							self.send_alert(Some(conn), data, None).await;
						},

						Command::AlertFromServer { data, target } => {
							self.send_alert(None, data, target).await;
						},

						Command::ForceRetryCamera { data } => {
							self.force_retry_camera(data).await;
						},

						Command::ListProcessingTypes { conn } => {
							self.list_processing_types(conn).await;
						},
					}
				}

				_ = interval.tick() => {
					self.clear_disconnected_channels().await;
					let save_data = self.get_persist_data().await;
					self.file_manager.save_server_state(save_data).await;
				}
			}
		}

		Ok(())
	}
}
