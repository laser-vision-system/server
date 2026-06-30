use actix_web::{web, get, HttpResponse, http::header::ContentType, post, Responder, HttpRequest};
use std::sync::Mutex;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CameraInfo {
    pub intensity: Mutex<f32>,
    pub deviation: Mutex<f32>,// <- Mutex is necessary to mutate safely across threads
}
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CameraInfoJson {
    pub intensity: Mutex<f32>,
    pub deviation: Mutex<f32>,// <- Mutex is necessary to mutate safely across threads
}
#[get("/camera_info")]
pub async fn get_camera_info(data: web::Data<CameraInfo>) -> impl Responder {
    let response = CameraInfoJson{
        intensity: Mutex::from(data.intensity.lock().unwrap().clone()),
        deviation: Mutex::from(data.deviation.lock().unwrap().clone())
    };
    HttpResponse::Ok().content_type(ContentType::json()).json(response)
}
#[post("/camera_info")]
pub async fn post_camera_info(data: web::Data<CameraInfo>, json: web::Json<CameraInfo>) -> impl Responder {
    let mut intensity = data.intensity.lock().unwrap();
    let mut deviation = data.deviation.lock().unwrap();
    *intensity = json.intensity.lock().unwrap().clone();
    *deviation = json.deviation.lock().unwrap().clone();
    HttpResponse::Ok().body("Camera Info upload successful")
}
