use actix_web::web;
use std::sync::Mutex;
use serde::Deserialize;

pub struct SetPoint {
    pub setpoint: Mutex<i32>, // <- Mutex is necessary to mutate safely across threads
}
#[derive(Deserialize)]
pub struct SetPointJson {
    pub setpoint: Mutex<i32>, // <- Mutex is necessary to mutate safely across threads
}
pub async fn get_set_point(data: web::Data<SetPoint>) -> String {
    let setpoint = data.setpoint.lock().unwrap();
    format!("the setpoint is: {setpoint}")
}
pub async fn set_set_point(data: web::Data<SetPoint>, json: web::Json<SetPointJson>) -> String {
    let mut setpoint = data.setpoint.lock().unwrap();
    *setpoint = *json.setpoint.lock().unwrap();
    format!("setpoint set too: {setpoint}")
}