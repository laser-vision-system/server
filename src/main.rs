mod server;
mod gui_handler;
mod edge_handler;

use std::io;
use actix_files::NamedFile;
use actix_web::{App, Error, HttpRequest, HttpResponse, HttpServer, Responder, middleware, web};
use tokio::{
    task::{spawn, spawn_local},
    try_join,
};
use self::server::{Server, ServerHandle};

pub type ConnId = u64;
pub type Msg = String;

async fn gui_ws(
    req: HttpRequest,
    stream: web::Payload,
    server: web::Data<ServerHandle>,
) -> Result<HttpResponse, Error> {
    let (res, session, msg_stream) = actix_ws::handle(&req, stream)?;

    Ok(res)
}

async fn edge_ws(
    req: HttpRequest,
    stream: web::Payload,
    server: web::Data<ServerHandle>,
) -> Result<HttpResponse, Error> {
    let (res, session, msg_stream) = actix_ws::handle(&req, stream)?;

    Ok(res)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> io::Result<()> {
    let (server, server_tx) = Server::new();

    let http_server = HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(server_tx.clone()))
            .service(web::resource("/gui_ws").route(web::get().to(gui_ws)))
            .service(web::resource("/edge_ws").route(web::get().to(edge_ws)))
    })
        .workers(2)
        .bind(("127.0.0.1",8080))?
        .run();

    Ok(())
}
