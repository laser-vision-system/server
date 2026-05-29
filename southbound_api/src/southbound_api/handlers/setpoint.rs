use actix_web::{web, get, Responder, HttpResponse, http::header::ContentType};
use std::sync::Mutex;
use serde::Deserialize;

pub struct SetPoint {
    pub setpoint: Mutex<bool>, // <- Mutex is necessary to mutate safely across threads
}
#[get("/setpoint")]
pub async fn get_set_point(data: web::Data<SetPoint>) -> HttpResponse {
    let mut setpoint = data.setpoint.lock().unwrap();
    let setpoint_response = setpoint.clone();
    *setpoint = false;
    HttpResponse::Ok()
        .content_type(ContentType::xml())
        .body(setpoint_response.to_string())
}