use actix_web::{web, post, Responder, HttpResponse, http::header::ContentType};
use std::sync::Mutex;
use serde::Deserialize;

use std::io::Write;
use actix_multipart::Multipart;
use actix_web::{middleware, web, App, Error, HttpResponse, HttpServer};

*
pub async fn save_file(mut payload: Multipart, file_path: String) -> Option<bool> {
    // iterate over multipart stream
    while let Ok(Some(mut field)) = payload.try_next().await {
        let content_type = field.content_disposition().unwrap();
        //let filename = content_type.get_filename().unwrap();
        let filepath = format!(".{}", file_path);

        // File::create is blocking operation, use threadpool
        let mut f = web::block(|| std::fs::File::create(filepath))
            .await
            .unwrap();

        // Field in turn is stream of *Bytes* object
        while let Some(chunk) = field.next().await {
            let data = chunk.unwrap();
            // filesystem operations are blocking, we have to use threadpool
            f = web::block(move || f.write_all(&data).map(|_| f))
                .await
                .unwrap();
        }
    }

    Some(true)

    https://users.rust-lang.org/t/file-upload-in-actix-web/64871/2