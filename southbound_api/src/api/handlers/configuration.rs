use actix_web::{web, get, HttpResponse, http::header::ContentType, post};
use std::sync::Mutex;
use serde::Deserialize;
use crate::api::handlers::camera_info::CameraInfo;

#[derive(Debug, Deserialize)]
pub struct Configuration {
    pub json: Mutex<String>, // <- Mutex is necessary to mutate safely across threads
}
#[get("/configuration")]
pub async fn get_configuration(data: web::Data<Configuration>) -> HttpResponse {
    let configuration = data.json.lock().unwrap(); // pull setpoint value out of web app
    HttpResponse::Ok()
        .content_type(ContentType::json())
        .body(configuration.to_string())
}
#[post("/configuration")]
pub async fn post_configuration(data: web::Data<Configuration>, json: web::Json<Configuration>) -> HttpResponse {
    let mut configuration = data.json.lock().unwrap(); // pull config value out of web app
    *configuration = json.json.lock().unwrap().to_string(); // sets config to false after responding
    HttpResponse::Ok().body("Configuration info posted")
}

