use actix_web::{HttpResponse, Responder, get, http::header::ContentType, post, web};
use serde::{Deserialize, Serialize};
use std::sync::Mutex;

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Configuration {
	pub brightness: Mutex<i32>,
	pub hue: Mutex<i32>,
	pub saturation: Mutex<i32>,
	pub framerate: Mutex<f32>,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
struct ConfigurationJson {
	pub brightness: Mutex<i32>,
	pub hue: Mutex<i32>,
	pub saturation: Mutex<i32>,
	pub framerate: Mutex<f32>,
}
#[get("/configuration")]
pub async fn get_configuration(data: web::Data<Configuration>) -> impl Responder {
	let response = ConfigurationJson {
		brightness: Mutex::from(data.brightness.lock().unwrap().clone()),
		hue: Mutex::from(data.hue.lock().unwrap().clone()),
		saturation: Mutex::from(data.saturation.lock().unwrap().clone()),
		framerate: Mutex::from(data.framerate.lock().unwrap().clone()),
	};
	HttpResponse::Ok()
		.content_type(ContentType::json())
		.json(response)
}
#[post("/configuration")]
pub async fn post_configuration(
	data: web::Data<Configuration>,
	json: web::Json<Configuration>,
) -> impl Responder {
	let mut brightness = data.brightness.lock().unwrap();
	let mut hue = data.hue.lock().unwrap();
	let mut saturation = data.saturation.lock().unwrap();
	let mut framerate = data.framerate.lock().unwrap();
	*brightness = json.brightness.lock().unwrap().clone();
	*hue = json.hue.lock().unwrap().clone();
	*saturation = json.saturation.lock().unwrap().clone();
	*framerate = json.framerate.lock().unwrap().clone();
	HttpResponse::Ok().body("Configuration upload successful")
}
