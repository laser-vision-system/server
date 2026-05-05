use std::sync::{Mutex};
use crate::southbound_api::data_structs::setpoint::{get_set_point, set_set_point, SetPoint};
use crate::southbound_api::data_structs::configuration::{get_configuration, set_configuration, Configuration};
use crate::southbound_api::data_structs::micro_computer_info::{get_micro_computer_info, set_micro_computer_info, MicroComputerInfo};
use crate::southbound_api::data_structs::t_cam_info::{get_t_cam_info, set_t_cam_info, TCameraInfo};
use actix_web::{web, App, HttpServer};
#[actix_web::main]
pub async fn southbound_api_interface() -> std::io::Result<()> {
    let setpoint = web::Data::new(SetPoint {
        setpoint: Mutex::new(3),
    });
    let t_camera_info = web::Data::new(TCameraInfo {
        t_camera_info: Mutex::new("".to_string())
    });
    let micro_computer_info = web::Data::new(MicroComputerInfo {
        micro_computer_info: Mutex::new("".to_string())
    });
    let configuration = web::Data::new(Configuration {
        configuration: Mutex::new("".to_string())
    });

    HttpServer::new(move || {
        App::new()
            .app_data(setpoint.clone())
            .app_data(t_camera_info.clone())
            .app_data(micro_computer_info.clone())
            .app_data(configuration.clone())
            .route("/set_point", web::get().to(get_set_point))
            .route("/set_point", web::post().to(set_set_point))
            .route("/t_cam_info", web::get().to(get_t_cam_info))
            .route("/t_cam_info", web::post().to(set_t_cam_info))
            .route("/micro_computer_info", web::get().to(get_micro_computer_info))
            .route("/micro_computer_info", web::post().to(set_micro_computer_info))
            .route("/configuration", web::get().to(get_configuration))
            .route("/configuration", web::post().to(set_configuration))

    })
        .bind("127.0.0.1:8080")?
        .run()
        .await
}