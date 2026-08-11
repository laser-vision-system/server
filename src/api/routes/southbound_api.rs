use crate::api::handlers::camera_info::post_camera_info;
use crate::api::handlers::configuration::get_configuration;
use crate::api::handlers::gui_update::get_gui_update;
use crate::api::handlers::image::upload_image;
use actix_web::web;

pub fn config(config: &mut web::ServiceConfig) {
	config
		.service(upload_image)
		.service(get_gui_update)
		.service(get_configuration)
		.service(post_camera_info);
}
