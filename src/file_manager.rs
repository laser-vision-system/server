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
use server_edge_datatypes as datatypes;
use server_edge_datatypes::edge::Settings;
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
	"Unable to deserialize settings",
];

const DEFAULT_CONFIG: &str = "./content/default_config.json";
const EDGE_LOGS: &str = "./content/edge.log";

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
		settings: Settings,
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

	async fn init_default_config(&mut self) -> SafeHttpResponse {

		let result = self.write_default_config(Settings::default()).await;

		if result.code != StatusCode::OK {
			return result;
		}

		let config = serde_json::to_string(&Settings::default()).unwrap();

		SafeHttpResponse {
			code: StatusCode::OK,
			json: true,
			body: Some(config),
			reason: None,
		}
	}

	async fn get_default_config(&mut self) -> SafeHttpResponse {
		let path = Path::new(DEFAULT_CONFIG);
		let check_file = File::options().read(true).open(path);

		let mut body: String = String::new();
		let mut file = match check_file {
			Err(reason) if reason.kind() == ErrorKind::NotFound =>
				return self.init_default_config().await,

			Err(reason) => return error_response(reason),

			Ok(file) => file,
		};

		let result = file.read_to_string(&mut body);

		if serde_json::from_str::<Settings>(body.as_str()).is_err() {
			return self.init_default_config().await;
		}

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

	async fn write_default_config(&mut self, json: Settings) -> SafeHttpResponse {
		let path = Path::new(DEFAULT_CONFIG);

		let check_file = File::options()
			.create(true)
			.write(true)
			.truncate(true)
			.open(path);

		if check_file.is_err() {
			return error_response(check_file.err().unwrap());
		}
		let mut file = check_file.unwrap();

		let Ok(write_settings) = serde_json::to_string(&json) else{
			return SafeHttpResponse {
				code: StatusCode::INTERNAL_SERVER_ERROR,
				json: false,
				body: None,
				reason: Some(REASONS[4]),
			};
		};

		let _ = write!(&mut file, "{write_settings}");

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
		let path = Path::new(EDGE_LOGS);
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

				Command::WriteDefaultConfig { tx, settings } => {
					let response = self.write_default_config(settings).await;
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
		settings: Settings,
	) -> SafeHttpResponse {
		let (tx, rx) = oneshot::channel();

		self.cmd_tx
			.send(Command::WriteDefaultConfig { tx, settings })
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
