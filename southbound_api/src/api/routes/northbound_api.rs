use actix_web::web;
use crate::api::handlers::camera_info::get_camera_info;
use crate::api::handlers::configuration::post_configuration;
use crate::api::handlers::image::{download_image};
use crate::api::handlers::setpoint::{post_set_point};

pub fn config(config: &mut web::ServiceConfig){
    config
        .service(download_image)
        .service(post_gui_update)
        .service(post_configuration)
        .service(get_camera_info);
}