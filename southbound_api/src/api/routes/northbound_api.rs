use actix_web::web;
use crate::api::handlers::image::{download_image};
use crate::api::handlers::setpoint::{post_set_point};

pub fn config(config: &mut web::ServiceConfig){
    config
        .service(download_image)
        .service(post_set_point);
}