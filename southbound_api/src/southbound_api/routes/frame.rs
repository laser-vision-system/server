use actix_web::web;
use crate::southbound_api::handlers::frame::post_frame;
pub fn config(config: &mut web::ServiceConfig){
    config
        .service(
            web::scope("/frame")
                .service(post_frame)
        );
}