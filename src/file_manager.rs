use std::{
	fs::File,
	io,
	io::{Error, ErrorKind, Read as _, Write as _},
	path::Path,
};
use std::fs::rename;
use actix_web::{
	HttpResponse,
	http::{StatusCode, header::ContentType},
};
use api::edge::Settings;
use api::shared::ConnId;
use tokio::sync::{mpsc, oneshot};
use crate::logger::logger;
use crate::server_persist_data::PersistentServerData;

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

const CONTENT_FOLDER: &str = "content";

const DEFAULT_CONFIG: &str = "./content/default_config.json";
const EDGE_LOGS: &str = "./content/edge.log";
const EDGE_OLD_LOGS: &str = "./content/edge.old.log";
const SERVER_LOGS: &str = "./content/server.log";
const SERVER_OLD_LOGS: &str = "./content/server.old.log";
const MAX_LOG_SIZE: u64 = 1024 * 50;

const SERVER_STATE: &str =  "./content/server_state.json";

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
	if let Some(reason) = unformatted.reason {
		return HttpResponse::build(unformatted.code)
			.reason(reason)
			.finish()
	}

	if unformatted.body.is_none() {
		return HttpResponse::build(unformatted.code).finish();
	}

	if unformatted.json {
		return if let Some(body) = unformatted.body {
			HttpResponse::Ok()
				.content_type(ContentType::json())
				.body(body)
		} else {
			HttpResponse::build(unformatted.code)
				.finish()
		}
	}

	if let Some(body) = unformatted.body {
		return HttpResponse::build(unformatted.code)
			.content_type(ContentType::plaintext())
			.body(body)
	}

	HttpResponse::build(unformatted.code).finish()
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
		conn_id: ConnId,
		logs: Vec<String>,
	},

	WriteServerLogs {
		logs: Vec<String>
	},

	SaveServerState {
		state: PersistentServerData,
	},

	LoadServerState {
		tx: oneshot::Sender<PersistentServerData>,
	}
}

#[derive(Debug)]
pub struct FileManager {
	cmd_rx: mpsc::UnboundedReceiver<Command>,
}

impl FileManager {
	pub fn new() -> (Self, FileManagerHandle) {
		Self::assert_content_folder();
		let (tx, rx) = mpsc::unbounded_channel();
		logger().init_filesystem(FileManagerHandle {cmd_tx: tx.clone()});
		(Self { cmd_rx: rx }, FileManagerHandle { cmd_tx: tx })
	}
	
	fn assert_content_folder() {
		let result = std::fs::create_dir_all(Path::new(CONTENT_FOLDER));
		if result.is_ok() {
			return
		}
		eprintln!("Could not create content folder!");
		panic!("Could not create content folder!");
	}

	async fn init_default_config(&mut self) -> SafeHttpResponse {

		let result = self.write_default_config(Settings::default()).await;

		if result.code != StatusCode::OK {
			return result;
		}

		let config = serde_json::to_string(&Settings::default()).expect("Unable to serialize Settings");

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

		let mut file;
		match check_file {
			Ok(value) => {
				file = value;
			}
			Err(err) => {
				return error_response(err);
			}
		}

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
		conn_id: ConnId,
		logs: Vec<String>,
	) -> SafeHttpResponse {
		let path = Path::new(EDGE_LOGS);
		let check_file = File::options().append(true).create(true).open(path);

		let mut file;
		match check_file {
			Ok(value) => {
				file = value;
			}
			Err(err) => {
				return error_response(err);
			}
		}

		for log in logs {
			let _result = writeln!(file, "{conn_id}: {log}");
		}

        let _ = file.sync_all();

        let size: u64;

        match file.metadata() {
            Ok(metadata) => {
                size = metadata.len();
            }

            Err(_) => {
                size = file.bytes().count() as u64;
            }
        }

        if size > MAX_LOG_SIZE {
            let _ = rename(path, Path::new(EDGE_OLD_LOGS));
        }


        SafeHttpResponse {
			code: StatusCode::OK,
			json: false,
			body: None,
			reason: None,
		}
	}

	async fn write_server_logs(&mut self, logs: Vec<String>) {
		let path = Path::new(SERVER_LOGS);

		let check_file = File::options().append(true).create(true).open(path);

		let mut file;
		match check_file {
			Ok(value) => {
				file = value;
			}
			Err(err) => {
				return;
			}
		}

		for log in logs {
			let _result = writeln!(file, "{log}");
		}

		let _ = file.sync_all();

		let size: u64;
		match file.metadata() {
			Ok(metadata) => {
				size = metadata.len();
			}
			Err(_) => {
				size = file.bytes().count() as u64;
			}
		}

		if size > MAX_LOG_SIZE {
			let _ = rename(path, Path::new(EDGE_OLD_LOGS));
		}
	}

	async fn save_server_state(&mut self, state: PersistentServerData) {
		let path = Path::new(SERVER_STATE);
		let check_file = File::options()
			.create(true)
			.write(true)
			.truncate(true)
			.open(path);

		let mut file;
		match check_file {
			Ok(value) => {
				file = value;
			}
			Err(err) => {
				logger().log_warn(format!("Could not open server_state.json due to: {:?}", err).as_str());
				return
			}
		}

		let json = serde_json::to_string(&state).expect("Unable to serialize PersistentServerData");
		let result = writeln!(&mut file, "{json}");

		if let Err(err) = result {
			logger().log_warn(format!("Could not write to server_state.json due to: {:?}", err).as_str());
		}

		return;
	}

	async fn load_server_state(&mut self) -> PersistentServerData {
		let path = Path::new(SERVER_STATE);
		let check_file = File::options().read(true).open(path);

		let mut file;
		match check_file {
			Ok(value) => {
				file = value;
			}
			Err(err) => {
				logger().log_warn(format!("Could not open server_state.json due to: {:?}", err).as_str());
				return PersistentServerData::default()
			}
		}


		let mut body: String = String::new();
		let result = file.read_to_string(&mut body);

		if let Err(err) =  result {
			logger().log_warn(format!("Could not open server_state.json due to: {:?}", err).as_str());
			return PersistentServerData::default()
		}

		let data = serde_json::from_str::<PersistentServerData>(&body);

		return match data {
			Ok(data) => {
				data
			}
			Err(err) => {
				logger().log_warn(format!("Could not parse server_state.json due to: {:?}", err).as_str());
				PersistentServerData::default()
			}
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

				Command::WriteEdgeLogs { tx, conn_id, logs } => {
					let response = self.write_edge_logs(conn_id, logs).await;
					let _ = tx.send(response);
				},

				Command::WriteServerLogs { logs } => {
					self.write_server_logs(logs).await;
				}

				Command::SaveServerState { state } => {
					self.save_server_state(state).await;
				}

				Command::LoadServerState { tx } => {
					let result = self.load_server_state().await;

					let _ = tx.send(result);
				}
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

		self.cmd_tx
			.send(Command::GetDefaultConfig { tx })
			.expect("File manager thread closed");

		rx.await.expect("Failed to receive response")
	}

	pub async fn change_default_config(
		&self,
		settings: Settings,
	) -> SafeHttpResponse {
		let (tx, rx) = oneshot::channel();

		self.cmd_tx
			.send(Command::WriteDefaultConfig { tx, settings })
			.expect("File manager thread closed");

		rx.await.expect("Failed to receive response")
	}

	pub async fn write_edge_logs(
		&self,
		conn_id: ConnId,
		logs: Vec<String>,
	) -> SafeHttpResponse {
		let (tx, rx) = oneshot::channel();

		self.cmd_tx
			.send(Command::WriteEdgeLogs { tx, conn_id, logs })
			.expect("File manager thread closed");

		rx.await.expect("Failed to receive response")
	}

	pub async fn write_server_logs(
		&self,
		logs: Vec<String>
	) {
		self.cmd_tx
			.send(Command::WriteServerLogs { logs })
			.expect("File manager thread closed");
	}

	pub async fn save_server_state(&self, state: PersistentServerData) {
		self.cmd_tx
			.send(Command::SaveServerState { state })
			.expect("File manager thread closed");
	}

	pub async fn load_server_state(&self) -> PersistentServerData {
		let (tx, rx) = oneshot::channel();

		self.cmd_tx
			.send(Command::LoadServerState { tx })
			.expect("File manager thread closed");

		rx.await.expect("Failed to receive response")
	}
}
