use std::sync::Mutex;
use actix_web::web;
use actix_web::web::Json;
use serde::Deserialize;
pub struct Configuration {
    pub configuration: Mutex<String>, // <- Mutex is necessary to mutate safely across threads
}
#[derive(Deserialize)]
pub struct ConfigurationJson {
    pub configuration: String, // <- Mutex is necessary to mutate safely across threads
}
pub async fn get_configuration(data: web::Data<Configuration>) -> String {
    let setpoint = data.configuration.lock().unwrap();
    format!("{:?}", setpoint)
}
pub async fn set_configuration(data: web::Data<Configuration>, json: Json<ConfigurationJson>) -> String {
    let mut setpoint = data.configuration.lock().unwrap();
    *setpoint = json.configuration.clone();
    format!("{:?}", setpoint)
}