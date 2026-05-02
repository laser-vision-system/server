use actix_web::{post, HttpResponse, Responder};
#[post("/micro_computer_info")]
async fn post_micro_computer_info(micro_computer_info: String) -> impl Responder {
    HttpResponse::Ok().json(micro_computer_info)
}