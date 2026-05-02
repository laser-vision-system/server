use crate::southbound_api::data_structs::setpoint::get_setpoint;
use crate::southbound_api::data_structs::configuration::get_configuration;
use crate::southbound_api::data_structs::micro_computer_info::post_micro_computer_info;
use crate::southbound_api::data_structs::t_cam_info::post_t_cam_info;
use actix_web::{App, HttpServer, };

#[actix_web::main]
pub async fn southbound_api_interface() -> std::io::Result<()> {
    HttpServer::new(|| {
        App::new()
            .service(get_setpoint)
            .service(get_configuration)
            .service(post_micro_computer_info)
            .service(post_t_cam_info)
    })
        .bind("127.0.0.1:8080")?
        .run()
        .await
}