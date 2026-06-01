use std::sync::{Mutex};
use actix_multipart::form::tempfile::TempFileConfig;
use crate::southbound_api::handlers::setpoint::{SetPoint};
use crate::southbound_api::handlers::configuration::{Configuration};
use crate::southbound_api::routes::southbound_api::config as southbound_config;
use crate::southbound_api::routes::northbound_api::config as northbound_config;
use actix_web::{middleware, web, App, HttpServer};
#[actix_web::main]
pub async fn api_interface() -> std::io::Result<()> {

    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info")); //debug logs
    log::info!("creating temporary upload directory");
    std::fs::create_dir_all("./tmp")?;
    let setpoint = web::Data::new(SetPoint {
        setpoint: Mutex::new(false), // creates data storage object for setpoint
    });
    let configuration = web::Data::new(Configuration {
        json: Mutex::new("".parse().unwrap()),
    });
    HttpServer::new(move || {
        App::new()
            .app_data(setpoint.clone())
            .app_data(configuration.clone())
            .wrap(middleware::Logger::default())
            .app_data(TempFileConfig::default().directory("./tmp"))
            .configure(southbound_config)
            .configure(northbound_config)
    })
        .bind("127.0.0.1:8080")?
        .run()
        .await
}