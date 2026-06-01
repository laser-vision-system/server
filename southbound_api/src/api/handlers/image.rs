use actix_multipart::form::{tempfile::TempFile, MultipartForm};
use actix_web::{HttpResponse, put, Responder};
#[derive(Debug, MultipartForm)]
struct UploadForm {
    #[multipart(rename = "file")]
    files: Vec<TempFile>,
}
#[put("/image")]
async fn upload_image(
    MultipartForm(form): MultipartForm<UploadForm>,
) -> impl Responder {
    for f in form.files {
        let path = format!("./tmp/{}", f.file_name.unwrap());
        log::info!("saving to {path}");
        f.file.persist(path).unwrap();
    }
    HttpResponse::Ok()
}

