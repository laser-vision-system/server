use std::collections::HashMap;
use crate::server::commands::Command;
use actix_web::web::Bytes;
use api::{edge, gui, shared};
use api::shared::{CameraId, ConnId};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;
use crate::{Msg, WSSide};
use crate::error::ServerResult;
use crate::server::Server;

#[derive(Debug, Clone)]
pub struct ServerHandle {
    cmd_tx: UnboundedSender<Command>,
}

impl ServerHandle {
    pub fn new(cmd_tx: UnboundedSender<Command>) -> Self {
        ServerHandle { cmd_tx }
    }

    pub async fn connect(
        &self,
        conn_tx: UnboundedSender<Msg>,
        conn_id: ConnId,
        side: WSSide,
        img_tx: Option<UnboundedSender<Bytes>>,
    ) -> bool {
        let (res_tx, res_rx) = oneshot::channel();

        self.cmd_tx
            .send(Command::Connect {
                conn_tx,
                res_tx,
                conn_id,
                side,
                img_tx,
            })
            .expect("Server thread closed");

        res_rx.await.expect("Failed to receive response")
    }

    pub async fn disconnect(&self, conn: ConnId, side: WSSide) {
        self.cmd_tx
            .send(Command::Disconnect { conn, side })
            .expect("Server thread closed");
    }

    pub async fn get_edge_name(&self, conn_id: ConnId) -> ServerResult<String> {
        let (res_tx, res_rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::GetEdgeName {
                res_tx,
                conn_id,
            })
            .expect("Server thread closed");;

        res_rx.await.expect("Failed to receive response")
    }

    pub async fn register_cameras(
        &self,
        conn: ConnId,
        cameras: HashMap<CameraId, edge::NewCameraInfo>,
    ) {
        self.cmd_tx
            .send(Command::RegisterCameras { conn, cameras })
            .expect("Server thread closed");
    }

    pub async fn update_camera_status(
        &self,
        conn: ConnId,
        data: edge::UpdateCameraStatus,
    ) {
        self.cmd_tx
            .send(Command::UpdateCameraStatus { conn, data })
            .expect("Server thread closed");
    }

    pub async fn image_analysis(
        &self,
        conn: ConnId,
        analysis: edge::ImageAnalysis,
    ) {
        self.cmd_tx
            .send(Command::ImageAnalysis { conn, analysis })
            .expect("Server thread closed");
    }

    pub async fn setpoint(&self, target: Option<gui::Setpoint>) {
        self.cmd_tx.send(Command::Setpoint { target }).expect("Server thread closed");
    }

    pub async fn list_cameras(&self, conn: ConnId) {
        self.cmd_tx.send(Command::ListCameras { conn }).expect("Server thread closed");
    }

    pub async fn set_name_camera(&self, data: gui::SetNameCamera) {
        self.cmd_tx
            .send(Command::SetNameCamera {
                conn: data.conn_id,
                camera: data.camera_id,
                name: data.name,
            })
            .expect("Server thread closed");
    }

    pub async fn edge_camera_name_update(&self, conn: ConnId, data: edge::SetNameCamera) {
        self.cmd_tx.send(Command::EdgeCameraNameUpdate { conn, data }).expect("Server thread closed");
    }

    pub async fn start_viewing_camera(
        &self,
        conn: ConnId,
        data: gui::StartViewingCamera,
    ) {
        self.cmd_tx
            .send(Command::StartViewingCamera {
                gui_conn: conn,
                edge_conn: data.conn_id,
                camera: data.camera_id,
            })
            .expect("Server thread closed");
    }

    pub async fn stop_viewing_camera(&self, conn: ConnId) {
        self.cmd_tx
            .send(Command::StopViewingCamera { conn })
            .expect("Server thread closed");
    }

    pub async fn configuration(
        &self,
        conn: ConnId,
        data: shared::Configuration,
        side: WSSide,
    ) {
        self.cmd_tx
            .send(Command::Configuration {
                conn,
                config: data,
                side,
            })
            .expect("Server thread closed");
    }

    pub async fn view_camera_config(
        &self,
        conn: ConnId,
        data: shared::ViewCameraConfig,
    ) {
        self.cmd_tx
            .send(Command::ViewCameraConfig { conn, camera: data })
            .expect("Server thread closed");
    }

    pub async fn forget_camera(
        &self,
        conn: ConnId,
        data: shared::ForgetCameraRequest,
        side: WSSide,
    ) {
        self.cmd_tx
            .send(Command::ForgetCamera {
                conn,
                target: data,
                side,
            })
            .expect("Server thread closed");
    }

    pub async fn route_image(&self, conn: ConnId, bytes: Bytes) {
        self.cmd_tx
            .send(Command::RouteImage { conn, bytes })
            .expect("Server thread closed");
    }

    pub async fn set_camera_thresholds(
        &self,
        data: gui::SetCameraThresholds,
    ) {
        self.cmd_tx
            .send(Command::SetCameraThresholds { data })
            .expect("Server thread closed");
    }

    pub async fn set_default_thresholds(
        &self,
        data: gui::SetDefaultThreshold,
    ) {
        self.cmd_tx
            .send(Command::SetDefaultThresholds { data })
            .expect("Server thread closed");
    }

    pub async fn set_edge_processing_type(
        &self,
        data: gui::SetEdgeProcessingType,
    ) {
        self.cmd_tx
            .send(Command::SetEdgeProcessingType { data })
            .expect("Server thread closed");
    }

    pub async fn get_default_thresholds(&self, conn: ConnId) {
        self.cmd_tx
            .send(Command::GetDefaultThresholds { conn })
            .expect("Server thread closed");
    }

    pub async fn list_edges(&self, conn: ConnId) {
        self.cmd_tx.send(Command::ListEdges { conn }).expect("Server thread closed");
    }

    pub async fn list_disconnected_edges(&self, conn: ConnId) {
        self.cmd_tx.send(Command::ListDisconnectedEdges { conn }).expect("Server thread closed");
    }

    pub async fn forget_edge(&self, data: gui::ForgetEdge) {
        self.cmd_tx.send(Command::ForgetEdge { data }).expect("Server thread closed");
    }

    pub async fn set_edge_name(&self, data: gui::SetEdgeName) {
        self.cmd_tx.send(Command::SetEdgeName { data }).expect("Server thread closed");
    }

    pub async fn alert_from_edge(
        &self,
        conn: ConnId,
        data: edge::Alert,
    ) {
        self.cmd_tx
            .send(Command::AlertFromEdge { conn, data })
            .expect("Server thread closed");
    }

    pub async fn alert_from_server(
        &self,
        data: edge::Alert,
        target: Option<ConnId>,
    ) {
        self.cmd_tx
            .send(Command::AlertFromServer { data, target })
            .expect("Server thread closed");
    }

    pub async fn force_retry_camera(
        &self,
        data: gui::ForceRetryCamera,
    ) {
        self.cmd_tx
            .send(Command::ForceRetryCamera { data })
            .expect("Server thread closed");
    }

    pub async fn list_processing_types(&self, conn: ConnId) {
        self.cmd_tx
            .send(Command::ListProcessingTypes { conn })
            .expect("Server thread closed");
    }
}
