use actix_web::{get, HttpResponse, Responder};
#[get("/configuration")]
async fn get_configuration() -> impl Responder {
    HttpResponse::Ok().body("configuration")
}