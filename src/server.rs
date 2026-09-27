use std::{
	collections::{HashMap, HashSet},
	io,
};

use actix_web::web::Bytes;
use datatypes::{
	gui::CameraDetails,
	shared::{CameraId, ConnId},
};
use rand::{self, RngExt as _};
use server_edge_datatypes as datatypes;
use server_edge_datatypes::shared::{CameraStatus, Deviation};
use tokio::sync::{mpsc, mpsc::UnboundedSender, oneshot};

use crate::{Msg, WSSide};

#[derive(Debug)]
enum Command {
	Connect {
		conn_tx: UnboundedSender<Msg>,
		res_tx: oneshot::Sender<ConnId>,
		side: WSSide,
		img_tx: Option<UnboundedSender<Bytes>>,
	},

	Disconnect {
		conn: ConnId,
		side: WSSide,
	},

	RegisterCameras {
		conn: ConnId,
		cameras: HashMap<CameraId, datatypes::edge::NewCameraInfo>,
	},

	UpdateCameraStatus {
		conn: ConnId,
		data: datatypes::edge::UpdateCameraStatus,
	},

	ImageAnalysis {
		conn: ConnId,
		analysis: datatypes::edge::ImageAnalysis,
	},

	Setpoint {
		target: Option<datatypes::gui::Setpoint>,
	},

	ListCameras {
		conn: ConnId,
	},

	SetNameCamera {
		conn: ConnId,
		camera: CameraId,
		name: String,
	},

	StartViewingCamera {
		gui_conn: ConnId,
		edge_conn: ConnId,
		camera: CameraId,
	},

	StopViewingCamera {
		conn: ConnId,
	},

	ViewCameraConfig {
		conn: ConnId,
		camera: datatypes::shared::ViewCameraConfig,
	},

	Configuration {
		conn: ConnId,
		config: datatypes::shared::Configuration,
		side: WSSide,
	},

	ForgetCamera {
		conn: ConnId,
		target: datatypes::shared::ForgetCameraRequest,
		side: WSSide,
	},

	RouteImage {
		conn: ConnId,
		bytes: Bytes,
	},

	SetCameraThresholds {
		data: datatypes::gui::SetCameraThresholds,
	},

	SetDefaultThresholds {
		data: datatypes::gui::SetDefaultThreshold,
	},

	SetEdgeProcessingType {
		conn: ConnId,
		data: datatypes::gui::SetEdgeProcessingType,
	},

	GetDefaultThresholds {
		conn: ConnId,
	},

	ListEdges {
		conn: ConnId,
	},

	SetEdgeName {
		data: datatypes::gui::SetEdgeName,
	},

	AlertFromEdge {
		conn: ConnId,
		data: datatypes::edge::Alert,
	},

	AlertFromServer {
		target: Option<ConnId>,
		data: datatypes::edge::Alert,
	},

	ForceRetryCamera {
		data: datatypes::gui::ForceRetryCamera,
	},

	ListProcessingTypes {
		conn: ConnId,
	}
}

#[derive(Hash, Debug, Clone, Copy, Eq, PartialEq)]
struct UniqueCameraID {
	conn: ConnId,
	camera: CameraId,
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

	cmd_rx: mpsc::UnboundedReceiver<Command>,
}

impl Server {
	pub fn new() -> (Self, ServerHandle) {
		let (tx, rx) = mpsc::unbounded_channel();
		(
			Self {
				gui_sessions: HashMap::new(),
				gui_img_route: HashMap::new(),
				edge_sessions: HashMap::new(),
				edge_names: HashMap::new(),
				cameras: HashMap::new(),
				video_routes: HashMap::new(),
				default_warn_threshold: 0.9,
				default_alert_threshold: 0.8,
				cmd_rx: rx,
			},
			ServerHandle { cmd_tx: tx },
		)
	}

	async fn connect(
		&mut self,
		tx: UnboundedSender<Msg>,
		img_tx: Option<UnboundedSender<Bytes>>,
		side: WSSide,
	) -> ConnId {
		let id = rand::rng().random::<ConnId>();
		match side {
			WSSide::GUI => {
				self.gui_sessions.insert(id, tx);
				self.gui_img_route.insert(id, img_tx.unwrap());
			},
			WSSide::Edge => {
				self.edge_sessions.insert(id, tx);
				self.edge_names.insert(
					id,
					format!("Edge {}", self.edge_names.len()),
				);
				for gui in self.gui_sessions.clone().keys() {
					self.list_edges(*gui).await;
				}
			},
		}

		id
	}

	async fn disconnect(&mut self, conn_id: ConnId, side: WSSide) {
		match side {
			WSSide::Edge => {
				self.edge_sessions.remove(&conn_id);

				let mut cameras: HashSet<(ConnId, CameraId)> = HashSet::new();

				for camera in self.cameras.keys() {
					if camera.conn == conn_id {
						println!("disconnecting camera {}", camera.camera);
						cameras
							.insert(<(ConnId, CameraId)>::from(*camera));
					}
				}

				for camera in cameras {
					let new_status = datatypes::edge::UpdateCameraStatus {
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

	fn is_connected(&self, conn_id: ConnId, camera: CameraId) -> bool {
		let camera_id = UniqueCameraID {
			conn: conn_id,
			camera,
		};

		if self.cameras.contains_key(&camera_id) {
			match self.cameras[&camera_id].state {
				CameraStatus::Active => {
					return true;
				},

				_ => false,
			};
		}
		false
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
		cameras: HashMap<CameraId, datatypes::edge::NewCameraInfo>,
	) {
		for camera in cameras {
			let camera_id = camera.0;

			let mut warn_below_threshold = self.default_warn_threshold;
			let mut alert_below_threshold = self.default_alert_threshold;

			if self.cameras.contains_key(&UniqueCameraID {
				conn,
				camera: camera_id,
			}) {
				let old_details = &self
					.cameras[&UniqueCameraID {
						conn,
						camera: camera_id,
					}];
				warn_below_threshold = old_details.warn_below_threshold;
				alert_below_threshold = old_details.alert_below_threshold;
			}

			let camera_details = CameraDetails {
				name: camera.1.name,
				state: camera.1.status,
				warn_below_threshold,
				alert_below_threshold,
			};
			self.cameras.insert(
				UniqueCameraID {
					conn,
					camera: camera_id,
				},
				camera_details,
			);
		}
		for gui in self.gui_sessions.clone().keys() {
			self.list_cameras(*gui).await;
		}
	}

	async fn update_camera_status(
		&mut self,
		conn: ConnId,
		data: datatypes::edge::UpdateCameraStatus,
	) {
		let get_details = self.cameras.get(&UniqueCameraID {
			conn,
			camera: data.camera_id,
		});
		if get_details.is_none() {
			return;
		}
		let details = get_details.unwrap();
		let new_details = CameraDetails {
			name: details.name.clone(),
			state: data.status,
			warn_below_threshold: details.warn_below_threshold,
			alert_below_threshold: details.alert_below_threshold,
		};
		self.cameras.insert(
			UniqueCameraID {
				conn,
				camera: data.camera_id,
			},
			new_details,
		);
		match data.status {
			CameraStatus::Active => {},

			_ => self.disconnect_camera(conn, data.camera_id).await,
		}
	}

	async fn disconnect_camera(&mut self, conn: ConnId, camera: CameraId) {
		for gui in self.gui_sessions.clone().keys() {
			self.list_cameras(*gui).await;
		}

		let mut remove: HashSet<ConnId> = HashSet::new();

		for route in self.video_routes.keys() {
			if self.video_routes[route].0 == conn
				&& self.video_routes[route].1 == camera
			{
				remove.insert(*route);
			}
		}

		for route in remove {
			self.video_routes.remove(&route);
		}
	}

	async fn image_analysis(
		&mut self,
		conn: ConnId,
		analysis: datatypes::edge::ImageAnalysis,
	) {
		let gui_struct = datatypes::gui::ImageAnalysis {
			conn_id: conn.to_string(),
			camera_id: analysis.camera_id,
			deviation: analysis.deviation,
		};
		let mut msg = "ImageAnalysis:".to_owned();
		let json = serde_json::to_string(&gui_struct).unwrap();
		msg.push_str(&json);

		for gui in self.gui_sessions.values() {
			gui.send(msg.clone()).unwrap();
		}
	}

	async fn set_point(&mut self, target: Option<datatypes::gui::Setpoint>) {
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
		let edge_struct = datatypes::edge::Setpoint { camera_id: camera };

		let mut msg = "Setpoint:".to_owned();
		let json = serde_json::to_string(&edge_struct).unwrap();
		msg.push_str(&json);

		let target = &self.edge_sessions[&conn];
		target.send(msg.clone()).unwrap();
	}

	async fn list_cameras(&mut self, conn: ConnId) {
		let mut cameras_sendable: HashMap<String, CameraDetails> =
			HashMap::new();

		for camera in self.cameras.clone() {
			let key = format!("{:?}", camera.0);
			cameras_sendable.insert(key, camera.1);
		}

		let gui_struct = datatypes::gui::ListOfCameras {
			cameras: cameras_sendable,
		};

		let mut msg = "ListOfCameras:".to_owned();
		let json = serde_json::to_string(&gui_struct).unwrap();
		msg.push_str(&json);

		let target = self.gui_sessions.get(&conn);

		if let Some(target) = target {
			target.send(msg.clone()).unwrap();
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

		let edge_struct = datatypes::edge::SetNameCamera {
			camera_id: camera,
			name,
		};

		let mut msg = "SetNameCamera:".to_owned();
		let json = serde_json::to_string(&edge_struct).unwrap();
		msg.push_str(&json);

		let target = &self.edge_sessions[&conn_id];
		target.send(msg.clone()).unwrap();
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

		let edge_struct =
			datatypes::edge::StartViewingCamera { camera_id: camera };

		let mut msg = "StartViewingCamera:".to_owned();
		let json = serde_json::to_string(&edge_struct).unwrap();
		msg.push_str(&json);
		let target = &self.edge_sessions[&edge_conn];
		target.send(msg.clone()).unwrap();
	}

	async fn stop_viewing_camera(&mut self, conn: ConnId) {
		let c_video_routes = self.video_routes.clone();
		let mut c2_video_routes = self.video_routes.clone();
		let route = c_video_routes.get(&conn);
		if route.is_none() {
			return;
		}

		c2_video_routes.remove(&conn);
		self.video_routes = c2_video_routes;

		let route = route.unwrap();

		if self.camera_in_use(route.0, route.1) {
			return;
		}

		let edge_struct =
			datatypes::edge::StopViewingCamera { camera_id: route.1 };
		let mut msg = "StopViewingCamera:".to_owned();
		let json = serde_json::to_string(&edge_struct).unwrap();
		msg.push_str(&json);
		let target = &self.edge_sessions[&route.0];
		target.send(msg.clone()).unwrap();
	}

	async fn configuration(
		&mut self,
		conn: ConnId,
		config: datatypes::shared::Configuration,
		side: WSSide,
	) {
		match side {
			WSSide::GUI => {
				if !self.is_connected(config.conn_id, config.camera_id) {
					return;
				}
			},
			WSSide::Edge => {
				if !self.is_connected(conn, config.camera_id) {
					return;
				}
			},
		}

		let mut msg = "Configuration:".to_owned();

		let target: &UnboundedSender<Msg>;
		match side {
			WSSide::GUI => {
				let fwd_config = datatypes::gui::Configuration {
					conn_id: conn.to_string(),
					camera_id: config.camera_id,
					configuration: config.configuration,
				};

				let json = serde_json::to_string(&fwd_config).unwrap();
				msg.push_str(&json);
				target = &self.edge_sessions[&config.conn_id];
			},
			WSSide::Edge => {
				let fwd_config = datatypes::shared::Configuration {
					conn_id: conn,
					camera_id: config.camera_id,
					configuration: config.configuration,
				};
				let json = serde_json::to_string(&fwd_config).unwrap();
				msg.push_str(&json);
				target = &self.gui_sessions[&config.conn_id];
			},
		}
		target.send(msg.clone()).unwrap();
	}

	async fn view_camera_config(
		&mut self,
		conn: ConnId,
		camera: datatypes::shared::ViewCameraConfig,
	) {
		if !self.is_connected(camera.conn_id, camera.camera_id) {
			return;
		}

		let fwd_data: datatypes::shared::ViewCameraConfig =
			datatypes::shared::ViewCameraConfig {
				conn_id: conn,
				camera_id: camera.camera_id,
			};

		let mut msg = "ViewCameraConfig:".to_owned();
		let json = serde_json::to_string(&fwd_data).unwrap();
		msg.push_str(&json);
		let target = &self.edge_sessions[&camera.conn_id];
		target.send(msg.clone()).unwrap();
	}

	async fn forget_camera(
		&mut self,
		conn: ConnId,
		target: datatypes::shared::ForgetCameraRequest,
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
					&& !target.confirm {
						return;
					}
			},
		}
		if !self.cameras.contains_key(&camera) {
			return;
		}

		match self.cameras[&camera].state {
			CameraStatus::Active => {},

			_ => return,
		}

		let mut msg = "ForgetCamera:".to_owned();

		match side {
			WSSide::Edge => {
				let send = datatypes::shared::ForgetCameraRequest {
					conn_id: conn,
					camera_id: target.camera_id,
					confirm: target.confirm,
				};
				msg.push_str(&serde_json::to_string(&send).unwrap());
				self.gui_sessions[&target.conn_id]
					.send(msg.clone())
					.unwrap();

				if target.confirm {
					self.cameras.remove(&camera);
					for gui in self.gui_sessions.clone().keys() {
						self.list_cameras(*gui).await;
					}
				}
			},

			WSSide::GUI => {
				let send = datatypes::gui::ForgetCamera {
					conn_id: conn.to_string(),
					camera_id: target.camera_id,
					confirm: target.confirm,
				};
				msg.push_str(&serde_json::to_string(&send).unwrap());
				self.edge_sessions[&target.conn_id]
					.send(msg.clone())
					.unwrap();
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
			return;
		}

		let img = Bytes::copy_from_slice(&bytes[8..]);

		if img.is_empty() {
			return;
		}

		for gui in self.video_routes.clone() {
			if gui.1.0 == conn_id && gui.1.1 == camera_id {
				self.gui_img_route[&gui.0]
					.send(img.clone())
					.unwrap();
			}
		}
	}

	async fn set_camera_thresholds(
		&mut self,
		data: datatypes::gui::SetCameraThresholds,
	) {
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
			};
			self.cameras.insert(camera, new_details);

			for gui in self.gui_sessions.clone().keys() {
				self.list_cameras(*gui).await;
			}
		}
	}

	async fn set_default_thresholds(
		&mut self,
		data: datatypes::gui::SetDefaultThreshold,
	) {
		self.default_warn_threshold = data.warn_below_threshold;
		self.default_alert_threshold = data.alert_below_threshold;
	}

	async fn set_edge_processing(
		&mut self,
		_conn_id: ConnId,
		data: datatypes::gui::SetEdgeProcessingType,
	) {
		let mut msg = "SetProcessingType:".to_owned();
		let send = datatypes::edge::SetProcessingType {
			processing_type: data.processing_type,
		};
		msg.push_str(&serde_json::to_string(&send).unwrap());

		let target = self.edge_sessions.get(&data.edge_id);

		if let Some(target) = target {
			target.send(msg.clone()).unwrap();
		}
	}

	async fn get_default_thresholds(&mut self, conn_id: ConnId) {
		let mut msg = "DefaultThresholds:".to_owned();
		let send = datatypes::gui::DefaultThresholds {
			warn_below_threshold: self.default_warn_threshold,
			alert_below_threshold: self.default_alert_threshold,
		};
		msg.push_str(&serde_json::to_string(&send).unwrap());
		let target = self.gui_sessions.get(&conn_id);

		if let Some(target) = target {
			target.send(msg.clone()).unwrap();
		}
	}

	async fn list_edges(&mut self, conn_id: ConnId) {
		let mut msg = "ListOfEdges:".to_owned();
		let mut edge_names_copy = HashMap::new();

		for edge in self.edge_names.clone() {
			edge_names_copy.insert(edge.0.to_string(), edge.1);
		}

		let send = datatypes::gui::ListOfEdges {
			edges: edge_names_copy,
		};
		msg.push_str(&serde_json::to_string(&send).unwrap());
		let target = self.gui_sessions.get(&conn_id);
		if let Some(target) = target {
			target.send(msg.clone()).unwrap();
		}
	}

	async fn set_edge_name(&mut self, data: datatypes::gui::SetEdgeName) {
		if let std::collections::hash_map::Entry::Occupied(mut e) = self.edge_names.entry(data.edge_id) {
			e.insert(data.name);
		}
	}

	async fn send_alert(
		&mut self,
		conn_id: Option<ConnId>,
		alert: datatypes::edge::Alert,
		target: Option<ConnId>,
	) {
		let mut msg = "Alert:".to_string();
		let send = datatypes::gui::Alert {
			conn_id,
			camera_id: alert.camera_id,
			alert_code: alert.alert_code,
			alert_type: alert.alert_type,
			alert_description: alert.alert_description,
		};
		msg.push_str(&*serde_json::to_string(&send).unwrap());

		if let Some(target) = target {
			self.gui_sessions
				.get(&target)
				.unwrap()
				.send(msg.clone())
				.unwrap();
		} else {
			for gui in self.gui_sessions.clone() {
				gui.1.clone().send(msg.clone()).unwrap();
			}
		}
	}

	async fn force_retry_camera(&mut self, data: datatypes::gui::ForceRetryCamera) {
		let mut msg = "ForceRetryCamera:".to_owned();
		let send = datatypes::edge::ForceRetryCamera {
			camera_id: data.camera_id,
		};
		msg.push_str(&serde_json::to_string(&send).unwrap());
		let target = self.gui_sessions.get(&data.camera_id);
		if let Some(target) = target {
			target.send(msg.clone()).unwrap();
		}
	}

	async fn list_processing_types(&mut self, conn_id: ConnId) {
		let mut msg = "list_processing_types:".to_owned();
		let send = datatypes::gui::ProcessingTypes::default();
		msg.push_str(&serde_json::to_string(&send).unwrap());
		let target = self.gui_sessions.get(&conn_id);
		if let Some(target) = target {
			target.send(msg.clone()).unwrap();
		}
	}

	pub async fn run(mut self) -> io::Result<()> {
		while let Some(cmd) = self.cmd_rx.recv().await {
			match cmd {
				Command::Connect {
					conn_tx,
					res_tx,
					side,
					img_tx,
				} => {
					let conn_id = self.connect(conn_tx, img_tx, side).await;
					let _ = res_tx.send(conn_id);
				},

				Command::Disconnect { conn, side } => {
					self.disconnect(conn, side).await;
				},

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

				Command::SetEdgeProcessingType { conn, data } => {
					self.set_edge_processing(conn, data).await;
				},

				Command::GetDefaultThresholds { conn } => {
					self.get_default_thresholds(conn).await;
				},

				Command::ListEdges { conn } => {
					self.list_edges(conn).await;
				},

				Command::SetEdgeName { data } => {
					self.set_edge_name(data).await;
				},

				Command::AlertFromEdge { conn, data } => {
					self.send_alert(Some(conn), data, None).await;
				},

				Command::AlertFromServer { data, target } => {
					self.send_alert(None, data, target).await;
				},

				Command::ForceRetryCamera {data} => {
					self.force_retry_camera(data).await;
				}

				Command::ListProcessingTypes { conn } => {
					self.list_processing_types(conn).await;
				}
			}
		}
		Ok(())
	}
}

#[derive(Debug, Clone)]
pub struct ServerHandle {
	cmd_tx: UnboundedSender<Command>,
}

impl ServerHandle {
	pub async fn connect(
		&self,
		conn_tx: UnboundedSender<Msg>,
		side: WSSide,
		img_tx: Option<UnboundedSender<Bytes>>,
	) -> ConnId {
		let (res_tx, res_rx) = oneshot::channel();

		self.cmd_tx
			.send(Command::Connect {
				conn_tx,
				res_tx,
				side,
				img_tx,
			})
			.unwrap();

		res_rx.await.unwrap()
	}

	pub async fn disconnect(&self, conn: ConnId, side: WSSide) {
		self.cmd_tx
			.send(Command::Disconnect { conn, side })
			.unwrap();
	}

	pub async fn register_cameras(
		&self,
		conn: ConnId,
		cameras: HashMap<CameraId, datatypes::edge::NewCameraInfo>,
	) {
		self.cmd_tx
			.send(Command::RegisterCameras { conn, cameras })
			.unwrap();
	}

	pub async fn update_camera_status(
		&self,
		conn: ConnId,
		data: datatypes::edge::UpdateCameraStatus,
	) {
		self.cmd_tx
			.send(Command::UpdateCameraStatus { conn, data })
			.unwrap();
	}

	pub async fn image_analysis(
		&self,
		conn: ConnId,
		analysis: datatypes::edge::ImageAnalysis,
	) {
		self.cmd_tx
			.send(Command::ImageAnalysis { conn, analysis })
			.unwrap();
	}

	pub async fn setpoint(&self, target: Option<datatypes::gui::Setpoint>) {
		self.cmd_tx.send(Command::Setpoint { target }).unwrap();
	}

	pub async fn list_cameras(&self, conn: ConnId) {
		self.cmd_tx.send(Command::ListCameras { conn }).unwrap();
	}

	pub async fn set_name_camera(&self, data: datatypes::gui::SetNameCamera) {
		self.cmd_tx
			.send(Command::SetNameCamera {
				conn: data.conn_id,
				camera: data.camera_id,
				name: data.name,
			})
			.unwrap();
	}

	pub async fn start_viewing_camera(
		&self,
		conn: ConnId,
		data: datatypes::gui::StartViewingCamera,
	) {
		self.cmd_tx
			.send(Command::StartViewingCamera {
				gui_conn: conn,
				edge_conn: data.conn_id,
				camera: data.camera_id,
			})
			.unwrap();
	}

	pub async fn stop_viewing_camera(&self, conn: ConnId) {
		self.cmd_tx
			.send(Command::StopViewingCamera { conn })
			.unwrap();
	}

	pub async fn configuration(
		&self,
		conn: ConnId,
		data: datatypes::shared::Configuration,
		side: WSSide,
	) {
		self.cmd_tx
			.send(Command::Configuration {
				conn,
				config: data,
				side,
			})
			.unwrap();
	}

	pub async fn view_camera_config(
		&self,
		conn: ConnId,
		data: datatypes::shared::ViewCameraConfig,
	) {
		self.cmd_tx
			.send(Command::ViewCameraConfig { conn, camera: data })
			.unwrap();
	}

	pub async fn forget_camera(
		&self,
		conn: ConnId,
		data: datatypes::shared::ForgetCameraRequest,
		side: WSSide,
	) {
		self.cmd_tx
			.send(Command::ForgetCamera {
				conn,
				target: data,
				side,
			})
			.unwrap();
	}

	pub async fn route_image(&self, conn: ConnId, bytes: Bytes) {
		self.cmd_tx
			.send(Command::RouteImage { conn, bytes })
			.unwrap();
	}

	pub async fn set_camera_thresholds(
		&self,
		data: datatypes::gui::SetCameraThresholds,
	) {
		self.cmd_tx
			.send(Command::SetCameraThresholds { data })
			.unwrap();
	}

	pub async fn set_default_thresholds(
		&self,
		data: datatypes::gui::SetDefaultThreshold,
	) {
		self.cmd_tx
			.send(Command::SetDefaultThresholds { data })
			.unwrap();
	}

	pub async fn set_edge_processing_type(
		&self,
		conn: ConnId,
		data: datatypes::gui::SetEdgeProcessingType,
	) {
		self.cmd_tx
			.send(Command::SetEdgeProcessingType { conn, data })
			.unwrap();
	}

	pub async fn get_default_thresholds(&self, conn: ConnId) {
		self.cmd_tx
			.send(Command::GetDefaultThresholds { conn })
			.unwrap();
	}

	pub async fn list_edges(&self, conn: ConnId) {
		self.cmd_tx.send(Command::ListEdges { conn }).unwrap();
	}

	pub async fn set_edge_name(&self, data: datatypes::gui::SetEdgeName) {
		self.cmd_tx.send(Command::SetEdgeName { data }).unwrap();
	}

	pub async fn alert_from_edge(&self, conn: ConnId, data: datatypes::edge::Alert) {
		self.cmd_tx
			.send(Command::AlertFromEdge { conn, data })
			.unwrap();
	}

	pub async fn alert_from_server(&self, data: datatypes::edge::Alert, target: Option<ConnId>) {
		self.cmd_tx
			.send(Command::AlertFromServer { data, target })
			.unwrap();
	}

	pub async fn force_retry_camera(&self, data: datatypes::gui::ForceRetryCamera) {
		self.cmd_tx
			.send(Command::ForceRetryCamera { data })
			.unwrap();
	}

	pub async fn list_processing_types(&self, conn: ConnId) {
		self.cmd_tx
			.send(Command::ListProcessingTypes { conn })
			.unwrap();
	}
}
