use actix_web::{web, get, post, HttpResponse, http::header::ContentType};
use std::sync::Mutex;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct SetPoint {
    pub setpoint: Mutex<bool>, // <- Mutex is necessary to mutate safely across threads
}
#[get("/setpoint")]
pub async fn get_set_point(data: web::Data<SetPoint>) -> HttpResponse {
    let mut setpoint = data.setpoint.lock().unwrap(); // pull setpoint value out of web app
    let setpoint_response = setpoint.clone();
    *setpoint = false; // sets setpoint to false after responding
    HttpResponse::Ok()
        .content_type(ContentType::xml())
        .body(setpoint_response.to_string())
}

#[post("/setpoint")]
pub async fn post_set_point(data: web::Data<SetPoint>) -> HttpResponse {
    let mut setpoint = data.setpoint.lock().unwrap();
    *setpoint = true;
    HttpResponse::Ok().body("Next frame tagged as setpoint")
}