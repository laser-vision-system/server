use actix_web::{web, get, HttpResponse, http::header::ContentType};
use std::sync::Mutex;
use serde::Deserialize;
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
