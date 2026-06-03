use actix_web::{web, get, post, HttpResponse, http::header::ContentType};
use std::sync::Mutex;
use serde::Deserialize;

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GuiUpdate {
    pub setpoint: Mutex<bool>,
    pub config: Mutex<bool>,// <- Mutex is necessary to mutate safely across threads
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
struct GuiUpdateJson {
    pub setpoint: Mutex<bool>,
    pub config: Mutex<bool>,// <- Mutex is necessary to mutate safely across threads
}

#[get("/gui_update")]
pub async fn get_gui_update(data: web::Data<GuiUpdate>) -> HttpResponse {
    let response = GuiUpdateJson {
        setpoint: Mutex::from(data.setpoint.lock().unwrap().clone()),
        config: Mutex::from(data.config.lock().unwrap().clone())
    };
    *setpoint = false;
    *config = false;
    HttpResponse::Ok().content_type(ContentType::json()).json(response)
}

#[post("/gui_update")]
pub async fn post_gui_update(data: web::Data<GuiUpdate>, json: web::Json<GuiUpdate>) -> HttpResponse {
    let mut setpoint = data.setpoint.lock().unwrap();
    let mut config = data.config.lock().unwrap();
    if(json.setpoint.lock().unwrap() == true) {
        *setpoint = true;
    }
    if(json.config.lock().unwrap() == true) {
        *config = false;
    }
    HttpResponse::Ok().body("Configuration upload successful")
}
