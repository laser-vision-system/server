use actix_web::{web, App, HttpServer, Responder, HttpResponse};
use serde::{Deserialize, Serialize};
use std::sync::Mutex;

/// https://boxoflearn.com/rust-rest-apis-guide/ <- from this guide

/// Step 1: Define a User structure.
/// - `Serialize` and `Deserialize` allow us to convert between JSON and Rust structs.
/// - `Clone` helps us return user data safely.
#[derive(Serialize, Deserialize, Clone)]
struct User {
    id: u32,
    name: String,
}

/// Step 2: Define shared application state.
/// We’ll store all users inside a vector protected by a `Mutex` for thread safety.
struct AppState {
    users: Mutex<Vec<User>>,
}

/// CREATE: Add a new user (POST /users)
async fn create_user(data: web::Data<AppState>, new_user: web::Json<User>) -> impl Responder {
    let mut users = data.users.lock().unwrap();
    users.push(new_user.into_inner());
    HttpResponse::Ok().body("User created successfully!")
}

/// READ: Get all users (GET /users)
async fn get_users(data: web::Data<AppState>) -> impl Responder {
    let users = data.users.lock().unwrap();
    web::Json(users.clone()) // Convert Vec<User> into JSON response
}

/// UPDATE: Update a user’s name by ID (PUT /users/{id})
async fn update_user(
    data: web::Data<AppState>,
    path: web::Path<u32>,
    updated: web::Json<User>,
) -> impl Responder {
    let user_id = path.into_inner();
    let mut users = data.users.lock().unwrap();

    if let Some(user) = users.iter_mut().find(|u| u.id == user_id) {
        user.name = updated.name.clone();
        HttpResponse::Ok().body("User updated successfully!")
    } else {
        HttpResponse::NotFound().body("User not found!")
    }
}

/// DELETE: Remove a user by ID (DELETE /users/{id})
async fn delete_user(data: web::Data<AppState>, path: web::Path<u32>) -> impl Responder {
    let user_id = path.into_inner();
    let mut users = data.users.lock().unwrap();

    let original_len = users.len();
    users.retain(|u| u.id != user_id);

    if users.len() < original_len {
        HttpResponse::Ok().body("User deleted successfully!")
    } else {
        HttpResponse::NotFound().body("User not found!")
    }
}

/// MAIN: Start the server and register all routes
#[actix_web::main]
async fn main() -> std::io::Result<()> {
    println!("Server running on http://127.0.0.1:8080");

    let state = web::Data::new(AppState {
        users: Mutex::new(Vec::new()),
    });

    HttpServer::new(move || {
        App::new()
            .app_data(state.clone())
            .route("/users", web::post().to(create_user))
            .route("/users", web::get().to(get_users))
            .route("/users/{id}", web::put().to(update_user))
            .route("/users/{id}", web::delete().to(delete_user))
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}

