use actix_web::web;
use crate::api::handlers::camera_info::post_camera_info;
use crate::api::handlers::configuration::get_configuration;
use crate::api::handlers::image::upload_image;
use crate::api::handlers::setpoint::get_set_point;

pub fn config(config: &mut web::ServiceConfig){
    config
        .service(upload_image)
        .service(get_set_point)
        .service(get_configuration)
        .service(post_camera_info);
}