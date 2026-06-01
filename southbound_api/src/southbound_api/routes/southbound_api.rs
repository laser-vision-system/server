use actix_web::web;
use crate::southbound_api::handlers::image::upload_image;
use crate::southbound_api::handlers::setpoint::get_set_point;

pub fn config(config: &mut web::ServiceConfig){
    config
        .service(upload_image)
        .service(get_set_point);
}