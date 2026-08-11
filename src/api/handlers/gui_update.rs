use actix_web::{HttpResponse, Responder, get, http::header::ContentType, post, web};
use serde::{Deserialize, Serialize};
use std::sync::Mutex;

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GuiUpdate {
	pub setpoint: Mutex<bool>,
	pub config: Mutex<bool>, // <- Mutex is necessary to mutate safely across threads
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
struct GuiUpdateJson {
	pub setpoint: Mutex<bool>,
	pub config: Mutex<bool>, // <- Mutex is necessary to mutate safely across threads
}

#[get("/gui_update")]
pub async fn get_gui_update(data: web::Data<GuiUpdate>) -> impl Responder {
	let response = GuiUpdateJson {
		setpoint: Mutex::from(data.setpoint.lock().unwrap().clone()),
		config: Mutex::from(data.config.lock().unwrap().clone()),
	};
	let mut setpoint = data.setpoint.lock().unwrap();
	let mut config = data.config.lock().unwrap();
	*setpoint = false;
	*config = false;
	HttpResponse::Ok()
		.content_type(ContentType::json())
		.json(response)
}

#[post("/gui_update")]
pub async fn post_gui_update(
	data: web::Data<GuiUpdate>,
	json: web::Json<GuiUpdate>,
) -> impl Responder {
	let mut setpoint = data.setpoint.lock().unwrap();
	let mut config = data.config.lock().unwrap();
	if json.setpoint.lock().unwrap().clone() {
		*setpoint = true;
	}
	if json.config.lock().unwrap().clone() {
		*config = true;
	}
	HttpResponse::Ok().body("Configuration upload successful")
}
