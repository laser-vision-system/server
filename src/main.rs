mod file_manager;
mod handler;
mod restful;
mod server;

use std::io;

use actix_web::{App, Error, HttpRequest, HttpResponse, HttpServer, web};
use tokio::{
	task::{spawn, spawn_local},
	try_join,
};

use self::server::{Server, ServerHandle};
use crate::file_manager::FileManager;

pub type Msg = String;

#[derive(Debug, Clone, Copy)]
enum WSSide {
	Edge,
	GUI,
}

async fn gui_ws(
	req: HttpRequest,
	stream: web::Payload,
	server: web::Data<ServerHandle>,
) -> Result<HttpResponse, Error> {
	let (res, session, msg_stream) = actix_ws::handle(&req, stream)?;

	println!("GUI connection!");
	spawn_local(handler::ws_handler(
		(**server).clone(),
		session,
		msg_stream,
		WSSide::GUI,
	));

	Ok(res)
}

async fn edge_ws(
	req: HttpRequest,
	stream: web::Payload,
	server: web::Data<ServerHandle>,
) -> Result<HttpResponse, Error> {
	let (res, session, msg_stream) = actix_ws::handle(&req, stream)?;

	println!("Edge connection!");
	spawn_local(handler::ws_handler(
		(**server).clone(),
		session,
		msg_stream,
		WSSide::Edge,
	));

	Ok(res)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> io::Result<()> {
	let (server, server_tx) = Server::new();
	let (file_manager, file_manager_tx) = FileManager::new();

	let file_manager = spawn(file_manager.run());
	let server = spawn(server.run());

	let http_server = HttpServer::new(move || {
		App::new()
			.app_data(web::Data::new(server_tx.clone()))
			.app_data(web::Data::new(file_manager_tx.clone()))
			.service(web::resource("/gui_ws").route(web::get().to(gui_ws)))
			.service(web::resource("/edge_ws").route(web::get().to(edge_ws)))
			.configure(restful::restful_routes)
	})
	.workers(2)
	.bind(("0.0.0.0", 8080))?
	.run();
	println!("HTTP server running on 0.0.0.0:8080");
	try_join!(
		http_server,
		async move { server.await.unwrap() },
		async move { file_manager.await.unwrap() }
	)?;

	Ok(())
}
