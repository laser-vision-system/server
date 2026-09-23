use std::{
	collections::HashSet,
	fs::File,
	io,
	io::{Error, ErrorKind, Read as _, Write as _},
	path::Path,
};

use actix_web::{
	HttpResponse,
	http::{StatusCode, header::ContentType},
};
use tokio::sync::{mpsc, oneshot};

#[derive(Debug)]
pub struct SafeHttpResponse {
	pub code: StatusCode,
	pub body: Option<String>,
	pub json: bool,
	pub reason: Option<&'static str>,
}

pub const REASONS: &[&str] = &[
	"Unable to find file on server",
	"Server does not have permission to access that file",
	"Unknown error when trying to access file",
	"File has invalid content",
];

fn error_response(result: Error) -> SafeHttpResponse {
	match result.kind() {
		ErrorKind::NotFound => SafeHttpResponse {
			code: StatusCode::NOT_FOUND,
			json: false,
			body: None,
			reason: Some(REASONS[0]),
		},

		ErrorKind::PermissionDenied => SafeHttpResponse {
			code: StatusCode::INTERNAL_SERVER_ERROR,
			json: false,
			body: None,
			reason: Some(REASONS[1]),
		},

		_ => SafeHttpResponse {
			code: StatusCode::INTERNAL_SERVER_ERROR,
			json: false,
			body: None,
			reason: Some(REASONS[2]),
		},
	}
}

pub fn fill_response(unformatted: SafeHttpResponse) -> HttpResponse {
	if unformatted.reason.is_some() {
		return HttpResponse::build(unformatted.code)
			.reason(unformatted.reason.unwrap())
			.finish();
	}

	if unformatted.body.is_none() {
		return HttpResponse::build(unformatted.code).finish();
	}

	if unformatted.json {
		return HttpResponse::Ok()
			.content_type(ContentType::json())
			.body(unformatted.body.unwrap());
	}

	HttpResponse::build(unformatted.code)
		.content_type(ContentType::plaintext())
		.body(unformatted.body.unwrap())
}

#[derive(Debug)]
enum Command {
	GetDefaultConfig {
		tx: oneshot::Sender<SafeHttpResponse>,
	},

	WriteDefaultConfig {
		tx: oneshot::Sender<SafeHttpResponse>,
		json: String,
	},

	WriteEdgeLogs {
		tx: oneshot::Sender<SafeHttpResponse>,
		logs: HashSet<String>,
	},
}

#[derive(Debug)]
pub struct FileManager {
	cmd_rx: mpsc::UnboundedReceiver<Command>,
}

impl FileManager {
	pub fn new() -> (Self, FileManagerHandle) {
		let (tx, rx) = mpsc::unbounded_channel();
		(Self { cmd_rx: rx }, FileManagerHandle { cmd_tx: tx })
	}

	async fn get_default_config(&mut self) -> SafeHttpResponse {
		let path = Path::new("./src/content/default_config.json");
		let check_file = File::options().read(true).open(path);
		if check_file.is_err() {
			return error_response(check_file.err().unwrap());
		}

		let mut file = check_file.unwrap();
		let mut body: String = String::new();
		let result = file.read_to_string(&mut body);

		if result.is_err() {
			return SafeHttpResponse {
				code: StatusCode::INTERNAL_SERVER_ERROR,
				json: false,
				body: None,
				reason: Some(REASONS[3]),
			};
		}

		SafeHttpResponse {
			code: StatusCode::OK,
			json: true,
			body: Some(body),
			reason: None,
		}
	}

	async fn write_default_config(&mut self, json: String) -> SafeHttpResponse {
		let path = Path::new("./src/content/default_config.json");
		let check_file = File::options()
			.create(true)
			.write(true)
			.truncate(true)
			.open(path);
		if check_file.is_err() {
			return error_response(check_file.err().unwrap());
		}
		let mut file = check_file.unwrap();

		let _result = write!(&mut file, "{json}");

		SafeHttpResponse {
			code: StatusCode::OK,
			json: false,
			body: None,
			reason: None,
		}
	}

	async fn write_edge_logs(
		&mut self,
		logs: HashSet<String>,
	) -> SafeHttpResponse {
		let path = Path::new("./src/content/edge.log");
		let check_file = File::options().append(true).create(true).open(path);
		if check_file.is_err() {
			return error_response(check_file.err().unwrap());
		}

		let mut file = check_file.unwrap();
		for log in logs {
			let _result = writeln!(file, "{log}");
		}

		SafeHttpResponse {
			code: StatusCode::OK,
			json: false,
			body: None,
			reason: None,
		}
	}

	pub async fn run(mut self) -> io::Result<()> {
		while let Some(cmd) = self.cmd_rx.recv().await {
			match cmd {
				Command::GetDefaultConfig { tx } => {
					let response = self.get_default_config().await;
					let _ = tx.send(response);
				},

				Command::WriteDefaultConfig { tx, json } => {
					let response = self.write_default_config(json).await;
					let _ = tx.send(response);
				},

				Command::WriteEdgeLogs { tx, logs } => {
					let response = self.write_edge_logs(logs).await;
					let _ = tx.send(response);
				},
			}
		}
		Ok(())
	}
}

#[derive(Debug, Clone)]
pub struct FileManagerHandle {
	cmd_tx: mpsc::UnboundedSender<Command>,
}

impl FileManagerHandle {
	pub async fn get_default_config(&self) -> SafeHttpResponse {
		let (tx, rx) = oneshot::channel();

		self.cmd_tx.send(Command::GetDefaultConfig { tx }).unwrap();

		rx.await.unwrap()
	}

	pub async fn change_default_config(
		&self,
		json: String,
	) -> SafeHttpResponse {
		let (tx, rx) = oneshot::channel();

		self.cmd_tx
			.send(Command::WriteDefaultConfig { tx, json })
			.unwrap();

		rx.await.unwrap()
	}

	pub async fn write_edge_logs(
		&self,
		logs: HashSet<String>,
	) -> SafeHttpResponse {
		let (tx, rx) = oneshot::channel();

		self.cmd_tx
			.send(Command::WriteEdgeLogs { tx, logs })
			.unwrap();

		rx.await.unwrap()
	}
}
