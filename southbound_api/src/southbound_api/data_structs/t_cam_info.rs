use actix_web::{post, HttpResponse, Responder};
#[post("/t_cam_info")]
async fn post_t_cam_info(t_cam_info: String) -> impl Responder {
    HttpResponse::Ok().json(t_cam_info)
}