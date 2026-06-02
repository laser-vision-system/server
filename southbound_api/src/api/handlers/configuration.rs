use actix_web::{web, get, HttpResponse, http::header::ContentType, post, Responder};
use std::sync::{Mutex};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Configuration {
    pub brightness: Mutex<f32>,
    pub framerate: Mutex<f32>,// <- Mutex is necessary to mutate safely across threads
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
struct ConfigurationJson {
    pub brightness: Mutex<f32>,
    pub framerate: Mutex<f32>,// <- Mutex is necessary to mutate safely across threads
}
#[get("/configuration")]
pub async fn get_configuration(data: web::Data<Configuration>) -> impl Responder {
    let response = ConfigurationJson {
        brightness: Mutex::from(data.brightness.lock().unwrap().clone()),
        framerate: Mutex::from(data.framerate.lock().unwrap().clone())
    };
    HttpResponse::Ok().content_type(ContentType::json()).json(response)
}
#[post("/configuration")]
pub async fn post_configuration(data: web::Data<Configuration>, json: web::Json<Configuration>) -> impl Responder {
    let mut brightness = data.brightness.lock().unwrap();
    let mut framerate = data.framerate.lock().unwrap();
    *brightness = json.brightness.lock().unwrap().clone();
    *framerate = json.framerate.lock().unwrap().clone();
    HttpResponse::Ok().body("Configuration upload successful")
}