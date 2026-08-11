use actix_files::NamedFile;
use actix_multipart::form::{MultipartForm, tempfile::TempFile};
use actix_web::{HttpResponse, Responder, get, put};

#[derive(Debug, MultipartForm)]
struct UploadForm {
	#[multipart(rename = "file")]
	files: Vec<TempFile>,
}

#[put("/image")]
async fn upload_image(MultipartForm(form): MultipartForm<UploadForm>) -> impl Responder {
	for f in form.files {
		// let path = format!("./tmp/{}", f.file_name.unwrap());
		let path = "./tmp/capture.png"; //At some point we will need to implement dynamic filenames
		log::info!("saving to {path}");
		f.file.persist(path).unwrap();
	}
	HttpResponse::Ok()
}

//will need to be changed to /image/{ID} at some point for multiple cameras
#[get("/image")]
async fn download_image() -> impl Responder {
	NamedFile::open_async("./tmp/capture.png").await
}
