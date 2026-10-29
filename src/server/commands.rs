use crate::error::ServerResult;
use crate::{Msg, WSSide};
use actix_web::web::Bytes;
use api::shared::{CameraId, ConnId};
use api::{edge, gui, shared};
use std::collections::HashMap;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;

#[derive(Debug)]
pub enum Command {
	Connect {
		conn_tx: UnboundedSender<Msg>,
		res_tx: oneshot::Sender<bool>,
		conn_id: ConnId,
		side: WSSide,
		img_tx: Option<UnboundedSender<Bytes>>,
	},

	Disconnect {
		conn: ConnId,
		side: WSSide,
	},

	GetEdgeName {
		res_tx: oneshot::Sender<ServerResult<String>>,
		conn_id: ConnId,
	},

	RegisterCameras {
		conn: ConnId,
		cameras: HashMap<CameraId, edge::NewCameraInfo>,
	},

	UpdateCameraStatus {
		conn: ConnId,
		data: edge::UpdateCameraStatus,
	},

	ImageAnalysis {
		conn: ConnId,
		analysis: edge::ImageAnalysis,
	},

	Setpoint {
		target: Option<gui::Setpoint>,
	},

	ListCameras {
		conn: ConnId,
	},

	ListDisconnectedEdges {
		conn: ConnId,
	},

	ForgetEdge {
		data: gui::ForgetEdge,
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
		camera: shared::ViewCameraConfig,
	},

	Configuration {
		conn: ConnId,
		config: shared::Configuration,
		side: WSSide,
	},

	ForgetCamera {
		conn: ConnId,
		target: shared::ForgetCameraRequest,
		side: WSSide,
	},

	RouteImage {
		conn: ConnId,
		bytes: Bytes,
	},

	SetCameraThresholds {
		data: gui::SetCameraThresholds,
	},

	SetDefaultThresholds {
		data: gui::SetDefaultThreshold,
	},

	SetEdgeProcessingType {
		data: gui::SetEdgeProcessingType,
	},

	GetDefaultThresholds {
		conn: ConnId,
	},

	ListEdges {
		conn: ConnId,
	},

	SetEdgeName {
		data: gui::SetEdgeName,
	},

	AlertFromEdge {
		conn: ConnId,
		data: edge::Alert,
	},

	AlertFromServer {
		target: Option<ConnId>,
		data: edge::Alert,
	},

	ForceRetryCamera {
		data: gui::ForceRetryCamera,
	},

	ListProcessingTypes {
		conn: ConnId,
	},

    EdgeCameraNameUpdate {
        conn: ConnId,
        data: edge::SetNameCamera
    },

    IsCameraCalibrated {
        conn: ConnId,
        data: gui::IsCameraCalibrated
    },

    CameraCalibrationUpdate {
        conn: ConnId,
        data: edge::CameraCalibrationUpdate
    },

    CameraCalibrationStatus {
        conn: ConnId,
        data: edge::CameraCalibrationStatus
    },
}
