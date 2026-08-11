use crate::api::handlers::camera_info::CameraInfo;
use crate::api::handlers::configuration::Configuration;
use crate::api::handlers::gui_update::GuiUpdate;
use crate::api::routes::northbound_api::config as northbound_config;
use crate::api::routes::southbound_api::config as southbound_config;
use actix_multipart::form::tempfile::TempFileConfig;
use actix_web::{App, HttpServer, middleware, web};
use std::sync::Mutex;
#[actix_web::main]
pub async fn api_interface() -> std::io::Result<()> {
	env_logger::init_from_env(env_logger::Env::new().default_filter_or("info")); //debug logs
	log::info!("creating temporary upload directory");
	std::fs::create_dir_all("./tmp")?;
	let gui_update = web::Data::new(GuiUpdate {
		setpoint: Mutex::new(false), // creates data storage object for setpoint
		config: Mutex::new(false),
	});
	let configuration = web::Data::new(Configuration {
		brightness: Mutex::new(1i32),
		hue: Mutex::new(1i32),
		saturation: Mutex::new(1i32),
		framerate: Mutex::new(1f32),
	});
	let camera_info = web::Data::new(CameraInfo {
		intensity: Mutex::new(10f32),
		deviation: Mutex::new(10f32),
	});

	HttpServer::new(move || {
		App::new()
			.app_data(gui_update.clone())
			.app_data(configuration.clone())
			.app_data(camera_info.clone())
			.wrap(middleware::Logger::default())
			.app_data(TempFileConfig::default().directory("./tmp"))
			.configure(southbound_config)
			.configure(northbound_config)
	})
	.bind("0.0.0.0:8080")?
	.run()
	.await
}
