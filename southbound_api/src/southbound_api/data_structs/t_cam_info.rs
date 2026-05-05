use std::sync::Mutex;
use actix_web::web;
use actix_web::web::Json;
use serde::Deserialize;

pub struct TCameraInfo {
    pub t_camera_info: Mutex<String>, // <- Mutex is necessary to mutate safely across threads
}

#[derive(Deserialize)]
pub struct TCameraInfoJson {
    pub t_camera_info: String, // <- Mutex is necessary to mutate safely across threads
}

pub async fn get_t_cam_info(data: web::Data<TCameraInfo>) -> String {
    let t_cam = data.t_camera_info.lock().unwrap();
    format!("{:?}", t_cam)
}

pub async fn set_t_cam_info(data: web::Data<TCameraInfo>, json: Json<TCameraInfoJson>) -> String {
    let mut t_cam = data.t_camera_info.lock().unwrap();
    *t_cam = json.t_camera_info.clone();
    format!("{:?}", t_cam)
}
