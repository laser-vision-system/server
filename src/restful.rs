
use crate::file_manager::{FileManagerHandle, fill_response};
use actix_web::{HttpResponse, Responder, get, post, web};
use api::restful;
use serde_json::json;

pub fn restful_routes(config: &mut web::ServiceConfig) {
	config
		.service(default_config)
		.service(is_api)
		.service(write_default_config)
		.service(edge_logs);
}

#[get("/default_config")]
async fn default_config(
	file_manager: web::Data<FileManagerHandle>,
) -> impl Responder {
	let response = file_manager.get_default_config().await;
	fill_response(response)
}

#[post("/write_default_config")]
async fn write_default_config(
	file_manager: web::Data<FileManagerHandle>,
	json: web::Json<restful::Config>,
) -> impl Responder {
	let response = file_manager
		.change_default_config(json.settings.clone())
		.await;
	fill_response(response)
}

#[post("/edge_logs")]
async fn edge_logs(
	file_manager: web::Data<FileManagerHandle>,
	json: web::Json<restful::Logs>,
) -> impl Responder {
	let response = file_manager
		.write_edge_logs(json.edge_id, json.logs.clone())
		.await;
	fill_response(response)
}

#[get("/is_api")]
async fn is_api() -> impl Responder {
	HttpResponse::Ok()
		.body("This is an API for the laser intensity analysis project")
}
