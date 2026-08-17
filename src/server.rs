use tokio::sync::{mpsc, oneshot};
use rand;
use std::{
    collections::{HashMap, HashSet},
    io,
};
use std::io::Read;
use env_logger::Target;
use rand::RngExt;
use server_edge_datatypes as datatypes;
use actix_web::web::Bytes;
use serde_json;
use server_edge_datatypes::{GUIImageAnalysis, GUISetpoint, StopViewingCamera, ViewCameraConfig};
use tokio::sync::mpsc::UnboundedSender;
use crate::{ConnId, Msg, WSSide};

#[derive(Debug)]
enum Command {
    Connect {
        conn_tx: mpsc::UnboundedSender<Msg>,
        res_tx: oneshot::Sender<ConnId>,
        side: WSSide,
        img_tx: Option<mpsc::UnboundedSender<Bytes>>,
    },

    Disconnect {
        conn: ConnId,
        side: WSSide
    },

    RegisterCameras {
        conn: ConnId,
        cameras: HashMap<u64, String>
    },

    UnregisterCamera {
        conn: ConnId,
        camera: u64
    },

    ImageAnalysis {
        conn: ConnId,
        analysis: datatypes::ImageAnalysis,
    },

    Setpoint {
        target: Option<datatypes::GUISetpoint>
    },

    ListCameras {
        conn: ConnId,
    },

    SetNameCamera {
        conn: ConnId,
        camera: u64,
        name: String
    },

    StartViewingCamera{
        gui_conn: ConnId,
        edge_conn: ConnId,
        camera: u64,
    },

    StopViewingCamera{
        conn: ConnId,
    },

    ViewCameraConfig{
        conn: ConnId,
        camera: datatypes::ViewCameraConfig,
    },

    Configuration{
        conn: ConnId,
        config: datatypes::Configuration,
        side: WSSide
    },

    ForgetCamera {
        conn: ConnId,
        target: datatypes::ForgetCamera,
        side: WSSide
    },

    RouteImage {
        conn: ConnId,
        bytes: Bytes,
    }
}

#[derive(Debug)]
pub struct Server {
    gui_sessions: HashMap<ConnId, mpsc::UnboundedSender<Msg>>,

    gui_img_route: HashMap<ConnId, mpsc::UnboundedSender<Bytes>>,

    edge_sessions: HashMap<ConnId, mpsc::UnboundedSender<Msg>>,

    cameras: HashMap<(ConnId, u64), (String, bool)>,

    video_routes: HashMap<ConnId, (ConnId, u64)>,

    cmd_rx: mpsc::UnboundedReceiver<Command>
}

impl Server {
    pub fn new() -> (Self, ServerHandle) {
        let (tx, rx) = mpsc::unbounded_channel();
        (
            Self {
                gui_sessions: HashMap::new(),
                gui_img_route: HashMap::new(),
                edge_sessions: HashMap::new(),
                cameras: HashMap::new(),
                video_routes: HashMap::new(),
                cmd_rx: rx
            },
            ServerHandle { cmd_tx: tx },
        )
    }

    async fn connect(&mut self, tx: mpsc::UnboundedSender<Msg>, img_tx: Option<mpsc::UnboundedSender<Bytes>>, side: WSSide) -> ConnId {
        let id = rand::rng().random::<ConnId>();
        match side {
            WSSide::GUI => {
                self.gui_sessions.insert(id, tx);
                self.gui_img_route.insert(id, img_tx.unwrap());
            }
            WSSide::Edge => {
                self.edge_sessions.insert(id, tx);
            }
        }

        id
    }

    async fn disconnect(&mut self, conn_id: ConnId, side: WSSide) {
        match side {
            WSSide::Edge => {
                self.edge_sessions.remove(&conn_id);

                let mut cameras:HashSet<(ConnId, u64)> = HashSet::new();

                for camera in self.cameras.keys() {
                    if camera.0 == conn_id {
                        cameras.insert(camera.clone());
                    }
                }

                for camera in cameras {
                    self.disconnect_camera(camera.0, camera.1).await;
                }

            }
            WSSide::GUI => {
                self.gui_sessions.remove(&conn_id);
            }
        }
    }

    fn is_connected(&self, conn_id: ConnId, camera: u64) -> bool {
        let camera_id = (conn_id, camera);
        if(self.cameras.contains_key(&camera_id)){
            if(self.cameras[&camera_id].1){
                return true;
            }
            return false;
        }
        return false;
    }

    fn camera_in_use(&self, conn_id: ConnId, camera: u64) -> bool {
        let camera_id = &(conn_id, camera);
        for route in self.video_routes.values(){
            if route == camera_id {
                return true;
            }
        }

        false
    }

    async fn register_cameras(&mut self, conn: ConnId, cameras: HashMap<u64, String>) {
        for camera in cameras {
            let camera_id = camera.0;
            let camera_name = camera.1;
            self.cameras.insert((conn, camera_id), (camera_name.clone(), true));

            let gui_struct = datatypes::CameraConnected {
                conn_id: conn,
                camera_id,
                camera_name
            };

            let mut msg = "CameraConnected:".to_string();
            let json = serde_json::to_string(&gui_struct).unwrap();
            msg.push_str(&json);

            for gui in self.gui_sessions.values() {
                gui.send(msg.clone()).unwrap();
            }
        }
    }

    async fn unregister_camera(&mut self, conn: ConnId, camera: u64) {
        let entry = self.cameras.get(&(conn, camera));
        let entry_2 = entry.unwrap();
        self.cameras.insert((conn, camera), (entry_2.0.clone(), false));
        self.disconnect_camera(conn, camera).await;
    }

    async fn disconnect_camera(&mut self, conn: ConnId, camera: u64) {
        let gui_struct = datatypes::CameraDisconnected{
            conn_id: conn,
            camera_id: camera,
        };

        let mut msg = "CameraDisconnected:".to_string();
        let json = serde_json::to_string(&gui_struct).unwrap();
        msg.push_str(&json);

        for gui in self.gui_sessions.values() {
            gui.send(msg.clone()).unwrap();
        }

        let mut remove: HashSet<ConnId> = HashSet::new();

        for route in self.video_routes.keys() {
            if self.video_routes[route].0 == conn && self.video_routes[route].1 == camera {
                remove.insert(route.clone());
            }
        }

        for route in remove {
            self.video_routes.remove(&route);
        }
    }

    async fn image_analysis(&mut self, conn: ConnId, analysis: datatypes::ImageAnalysis) {
        let gui_struct = datatypes::GUIImageAnalysis {
            conn_id: conn,
            camera_id: analysis.camera_id,
            deviation: analysis.deviation,
            intensity: analysis.intensity,
        };
        let mut msg = "ImageAnalysis:".to_string();
        let json = serde_json::to_string(&gui_struct).unwrap();
        msg.push_str(&json);

        for gui in self.gui_sessions.values() {
            gui.send(msg.clone()).unwrap();
        }
    }

    async fn set_point(&mut self, target: Option<datatypes::GUISetpoint>) {
        if let Some(target) = target {
            if(self.is_connected(target.conn_id, target.camera_id)){
                self.send_set_point(target.conn_id, target.camera_id).await;
            }
        } else {
            let mut targets: HashSet<(ConnId, u64)> = HashSet::new();

            for target in self.cameras.keys() {
                targets.insert(target.clone());
            }

            for target in targets {
                self.send_set_point(target.0, target.1).await;
            }
        }
    }

    async fn send_set_point(&mut self, conn: ConnId, camera: u64) {
        let edge_struct = datatypes::Setpoint {
            camera_id: camera,
        };

        let mut msg = "Setpoint:".to_string();
        let json = serde_json::to_string(&edge_struct).unwrap();
        msg.push_str(&json);

        let target = self.edge_sessions.get(&conn).unwrap();
        target.send(msg.clone()).unwrap();
    }

    async fn list_cameras(&mut self, conn: ConnId) {
        let gui_struct = datatypes::ListOfCameras{
            cameras: self.cameras.clone(),
        };

        let mut msg = "ListOfCameras:".to_string();
        let json = serde_json::to_string(&gui_struct).unwrap();
        msg.push_str(&json);

        let target = self.gui_sessions.get(&conn);

        if let Some(target) = target {
            target.send(msg.clone()).unwrap();
        }
    }

    async fn set_name_camera(&mut self, conn_id: ConnId, camera: u64, name: String) {
        if(!self.is_connected(conn_id, camera)){
            return
        }

        let edge_struct = datatypes::SetNameCamera {
            camera_id: camera,
            name
        };

        let mut msg = "SetNameCamera:".to_string();
        let json = serde_json::to_string(&edge_struct).unwrap();
        msg.push_str(&json);

        let target = self.edge_sessions.get(&conn_id).unwrap();
        target.send(msg.clone()).unwrap();
    }

    async fn start_viewing_camera(&mut self, gui_conn: ConnId, edge_conn: ConnId, camera: u64) {
        if(!self.is_connected(edge_conn, camera)){
            return;
        }

        if(self.video_routes.contains_key(&gui_conn)){
            if(self.video_routes[&gui_conn] == (edge_conn, camera)){
                return;
            }
            self.stop_viewing_camera(gui_conn).await;
        }

        let mut skip_camera_startup = false;

        if(self.camera_in_use(edge_conn, camera)) {
            skip_camera_startup = true;
        }

        self.video_routes.insert(gui_conn, (edge_conn, camera));

        if skip_camera_startup {
            return;
        }

        let edge_struct = datatypes::StartViewingCamera {
            camera_id: camera,
        };

        let mut msg = "StartViewingCamera:".to_string();
        let json = serde_json::to_string(&edge_struct).unwrap();
        msg.push_str(&json);
        let target = self.edge_sessions.get(&edge_conn).unwrap();
        target.send(msg.clone()).unwrap();
    }

    async fn stop_viewing_camera(&mut self, conn: ConnId) {
        let c_video_routes = self.video_routes.clone();
        let mut c2_video_routes = self.video_routes.clone();
        let route = c_video_routes.get(&conn).clone();
        if route == None {
            return
        }

        c2_video_routes.remove(&conn);
        self.video_routes = c2_video_routes;

        let route = route.unwrap();

        if(self.camera_in_use(route.0, route.1)){
            return;
        }

        let edge_struct = datatypes::StopViewingCamera{
            camera_id: route.1
        };
        let mut msg = "StopViewingCamera:".to_string();
        let json = serde_json::to_string(&edge_struct).unwrap();
        msg.push_str(&json);
        let target = self.edge_sessions.get(&route.0).unwrap();
        target.send(msg.clone()).unwrap();
    }

    async fn configuration(&mut self, conn: ConnId, config: datatypes::Configuration, side: WSSide) {
        match side {
            WSSide::GUI => {
                if(!self.is_connected(config.conn_id, config.camera_id)){
                    return
                }
            }
            WSSide::Edge => {
                if(!self.is_connected(conn, config.camera_id)){
                    return
                }
            }
        }

        let fwd_config: datatypes::Configuration = datatypes::Configuration{
            conn_id: conn,
            camera_id: config.camera_id,
            configuration: config.configuration,
        };
        let mut msg = "Configuration:".to_string();
        let json = serde_json::to_string(&fwd_config).unwrap();
        msg.push_str(&json);
        let target: &UnboundedSender<Msg>;
        match side {
            WSSide::GUI => {
                target = self.edge_sessions.get(&config.conn_id).unwrap();
            }
            WSSide::Edge => {
                target = self.gui_sessions.get(&config.conn_id).unwrap();
            }
        }
        target.send(msg.clone()).unwrap();
    }

    async fn view_camera_config(&mut self, conn: ConnId, camera: datatypes::ViewCameraConfig) {
        if(!self.is_connected(camera.conn_id, camera.camera_id)){
            return
        }

        let fwd_data: datatypes::ViewCameraConfig = datatypes::ViewCameraConfig{
            conn_id: conn,
            camera_id: camera.camera_id,
        };

        let mut msg = "ViewCameraConfig:".to_string();
        let json = serde_json::to_string(&fwd_data).unwrap();
        msg.push_str(&json);
        let target = self.edge_sessions.get(&camera.conn_id).unwrap();
        target.send(msg.clone()).unwrap();
    }

    async fn forget_camera(&mut self, conn: ConnId, target: datatypes::ForgetCamera, side: WSSide) {
        let camera: (ConnId, u64);
        match side {
            WSSide::GUI => {
                camera = (target.conn_id, target.camera_id);
            }

            WSSide::Edge => {
                camera = (conn, target.camera_id);
                if(!self.gui_sessions.contains_key(&target.conn_id)){
                    if(!target.confirm){
                        return
                    }
                }
            }
        }
        if(!self.cameras.contains_key(&camera)){
            return
        }

        if(!self.cameras[&camera].1){
            return
        }

        let mut msg = "ForgetCamera:".to_string();
        let send = datatypes::ForgetCamera{
            conn_id: conn,
            camera_id: target.camera_id,
            confirm: target.confirm,
        };
        msg.push_str(&*serde_json::to_string(&send).unwrap());

        match side {
            WSSide::Edge => {
                if target.confirm == true {
                    self.cameras.remove(&camera);
                    for gui in self.gui_sessions.values() {
                        gui.send(msg.clone()).unwrap();
                    }
                } else {
                    self.gui_sessions.get(&target.conn_id).unwrap().send(msg.clone()).unwrap();
                }

            }

            WSSide::GUI => {
                self.edge_sessions.get(&target.conn_id).unwrap().send(msg.clone()).unwrap();
            }
        }
    }

    async fn cam_id_from_bytes(&mut self, bytes: Bytes) -> u64 {
        let mut buf = [0u8; 8];
        buf[..8].copy_from_slice(&bytes[..8]);
        u64::from_be_bytes(buf)
    }

    async fn route_image(&mut self, conn_id: ConnId, bytes: Bytes) {
        let len = bytes.len();
        if len < 10 {
            return;
        }

        let camera_id: u64 = self.cam_id_from_bytes(bytes.clone()).await;

        if !self.camera_in_use(conn_id, camera_id){
            return
        }

        let img = Bytes::copy_from_slice(&bytes[9..]);

        if img.len() == 0 {
            return
        }

        for gui in self.video_routes.clone() {
            if gui.1.0 == conn_id && gui.1.1 == camera_id {
                self.gui_img_route.get(&gui.0).unwrap().send(img.clone()).unwrap();
            }
        }
    }

    pub async fn run(mut self) -> io::Result<()> {
        while let Some(cmd) = self.cmd_rx.recv().await {
            match cmd {
                Command::Connect { conn_tx, res_tx, side,img_tx} => {
                    let conn_id = self.connect(conn_tx, img_tx, side ).await;
                    let _ = res_tx.send(conn_id);
                }

                Command::Disconnect { conn, side } => {
                    self.disconnect(conn,side).await;
                }

                Command::RegisterCameras { conn, cameras} => {
                    self.register_cameras(conn, cameras).await;
                }

                Command::UnregisterCamera { conn, camera } => {
                    self.unregister_camera(conn, camera).await;
                }

                Command::ImageAnalysis { conn, analysis } => {
                    self.image_analysis(conn, analysis).await;
                }

                Command::Setpoint { target } => {
                    self.set_point(target).await;
                }

                Command::ListCameras {conn} => {
                    self.list_cameras(conn).await;
                }

                Command::SetNameCamera {conn,  camera, name } => {
                    self.set_name_camera(conn, camera, name).await;
                }

                Command::StartViewingCamera { gui_conn, edge_conn, camera} => {
                    self.start_viewing_camera(gui_conn,edge_conn, camera).await;
                }

                Command::StopViewingCamera { conn } => {
                    self.stop_viewing_camera(conn).await;
                }

                Command::Configuration { conn, config, side} => {
                    self.configuration(conn, config, side).await;
                }

                Command::ViewCameraConfig {conn, camera} => {
                    self.view_camera_config(conn, camera).await;
                }

                Command::ForgetCamera {conn, target, side} => {
                    self.forget_camera(conn, target, side).await;
                }

                Command::RouteImage {conn, bytes} => {
                    self.route_image(conn, bytes).await;
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
    pub async fn connect(&self, conn_tx: mpsc::UnboundedSender<Msg>, side: WSSide, img_tx: Option<mpsc::UnboundedSender<Bytes>>) -> ConnId {
        let (res_tx, res_rx) = oneshot::channel();

        self.cmd_tx
            .send(Command::Connect { conn_tx, res_tx, side, img_tx })
            .unwrap();

        res_rx.await.unwrap()
    }

    pub async fn disconnect(&self, conn: ConnId, side: WSSide) {
        self.cmd_tx.send(Command::Disconnect {conn,side}).unwrap();
    }

    pub async fn register_cameras(&self, conn: ConnId, cameras: HashMap<u64, String>) {
        self.cmd_tx.send(Command::RegisterCameras {conn,cameras}).unwrap();
    }

    pub async fn unregister_camera(&self, conn: ConnId, camera: u64) {
        self.cmd_tx.send(Command::UnregisterCamera {conn,camera}).unwrap();
    }

    pub async fn image_analysis(&self, conn: ConnId, analysis: datatypes::ImageAnalysis) {
        self.cmd_tx.send(Command::ImageAnalysis {conn,analysis}).unwrap();
    }

    pub async fn setpoint(&self, target: Option<datatypes::GUISetpoint>){
        self.cmd_tx.send(Command::Setpoint {target}).unwrap();
    }

    pub async fn list_cameras(&self, conn: ConnId) {
        self.cmd_tx.send(Command::ListCameras{conn}).unwrap();
    }

    pub async fn set_name_camera(&self, data: datatypes::GUISetNameCamera) {
        self.cmd_tx.send(Command::SetNameCamera {
            conn: data.conn_id,
            camera: data.camera_id,
            name: data.name
        }).unwrap();
    }

    pub async fn start_viewing_camera(&self, conn: ConnId, data: datatypes::GUIStartViewingCamera) {
        self.cmd_tx.send(Command::StartViewingCamera {
            gui_conn: conn,
            edge_conn: data.conn_id,
            camera: data.camera_id,
        }).unwrap();
    }

    pub async fn stop_viewing_camera(&self, conn: ConnId) {
        self.cmd_tx.send(Command::StopViewingCamera {conn}).unwrap();
    }

    pub async fn configuration(&self, conn: ConnId, data: datatypes::Configuration, side: WSSide) {
        self.cmd_tx.send(Command::Configuration {conn, config: data ,side}).unwrap();
    }

    pub async fn view_camera_config(&self, conn: ConnId, data: datatypes::ViewCameraConfig) {
        self.cmd_tx.send(Command::ViewCameraConfig {conn, camera: data}).unwrap();
    }

    pub async fn forget_camera(&self, conn: ConnId, data: datatypes::ForgetCamera, side: WSSide) {
        self.cmd_tx.send(Command::ForgetCamera {conn, target: data, side}).unwrap();
    }

    pub async fn route_image(&self, conn: ConnId, bytes: Bytes) {
        self.cmd_tx.send(Command::RouteImage {conn, bytes}).unwrap();
    }
}