use actix_web::{get, HttpResponse, Responder};
#[get("/setpoint")]
async fn get_setpoint() -> impl Responder {
    HttpResponse::Ok().body("setpoint")
}