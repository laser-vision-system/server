use std::sync::Mutex;
use actix_web::web;
use actix_web::web::Json;
use serde::Deserialize;

pub struct MicroComputerInfo {
    pub micro_computer_info: Mutex<String>, // <- Mutex is necessary to mutate safely across threads
}
#[derive(Deserialize)]
pub struct MicroComputerInfoJson {
    pub micro_computer_info: String, // <- Mutex is necessary to mutate safely across threads
}
pub async fn get_micro_computer_info(data: web::Data<MicroComputerInfo>) -> String {
    let setpoint = data.micro_computer_info.lock().unwrap();
    format!("{:?}", setpoint)
}
pub async fn set_micro_computer_info(data: web::Data<MicroComputerInfo>, json: Json<MicroComputerInfoJson>) -> String {
    let mut setpoint = data.micro_computer_info.lock().unwrap();
    *setpoint = json.micro_computer_info.clone();
    format!("{:?}", setpoint)
}