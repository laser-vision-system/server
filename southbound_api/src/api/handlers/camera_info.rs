use actix_web::{web, get, HttpResponse, http::header::ContentType, post};
use std::sync::Mutex;
use serde::{Deserialize, Serialize};
use crate::api::handlers::configuration::Configuration;

#[derive(Debug, Deserialize)]
pub struct CameraInfo {
    pub json: Mutex<String>, // <- Mutex is necessary to mutate safely across threads
}
#[get("/get_camera_info")]
pub async fn get_camera_info(data: web::Data<CameraInfo>) -> HttpResponse {
    let camera_info = data.json.lock().unwrap(); // pull setpoint value out of web app
    HttpResponse::Ok()
        .content_type(ContentType::json())
        .body(camera_info.to_string())
}
#[post("/post_camera_info")]
pub async fn post_camera_info(data: web::Data<CameraInfo>, json: web::Json<CameraInfo>) -> HttpResponse {
    let mut camera_info = data.json.lock().unwrap(); // pull setpoint value out of web app
    *camera_info = json.json.lock().unwrap().to_string(); // sets setpoint to false after responding
    HttpResponse::Ok().body("camera info posted")
}
