use actix_web::web;

pub fn restful_routes(config: &mut web::ServiceConfig) {
    config.service(); //Calls to restful functions go here
}