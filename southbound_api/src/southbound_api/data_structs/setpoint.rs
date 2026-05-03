use actix_web::{get, HttpResponse, Responder, web};


#[get("/setpoint")]
async fn get_setpoint(setpoint: String) -> impl Responder {
    HttpResponse::Ok().json(setpoint)
}