use crate::api::handlers::camera_info::get_camera_info;
use crate::api::handlers::configuration::post_configuration;
use crate::api::handlers::gui_update::post_gui_update;
use crate::api::handlers::image::download_image;
use actix_web::web;

pub fn config(config: &mut web::ServiceConfig) {
	config
		.service(download_image)
		.service(post_gui_update)
		.service(post_configuration)
		.service(get_camera_info);
}
