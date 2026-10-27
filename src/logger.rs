use std::{
    cell::RefCell,
    fmt::Display,
    mem,
    sync::{
        Mutex,
        OnceLock,
    },
};
use chrono::{
    Local,
    NaiveDate,
};
use serde::{
    Deserialize,
    Serialize,
};
use crate::file_manager::FileManagerHandle;

pub type LoggerRef = &'static LoggerGuard;

static LOGGER: OnceLock<LoggerGuard> = OnceLock::new();

pub struct LoggerGuard {
    inner: Mutex<RefCell<Logger>>,
}

pub fn logger() -> LoggerRef {
    LOGGER.get_or_init(LoggerGuard::new)
}

impl LoggerGuard {
    fn new() -> Self {
        Self {
            inner: Mutex::new(RefCell::new(Logger::new())),
        }
    }

    pub fn log_info(
        &self,
        message: &str,
    ) {
        let guard = match self.inner.lock() {
            Ok(cell) => cell,
            Err(why) => {
                eprintln!("Logger was poisoned!");
                why.into_inner()
            },
        };
        let mut logger = guard.borrow_mut();

        logger.log_info(message);
    }

    pub fn log_warn(
        &self,
        message: &str,
    ) {
        let guard = match self.inner.lock() {
            Ok(cell) => cell,
            Err(why) => {
                eprintln!("Logger was poisoned!");
                why.into_inner()
            },
        };
        let mut logger = guard.borrow_mut();

        logger.log_warn(message);
    }

    pub fn log_error<T>(
        &self,
        error: &T,
    ) where
        T: Display + ?Sized,
    {
        let guard = match self.inner.lock() {
            Ok(cell) => cell,
            Err(why) => {
                eprintln!("Logger was poisoned!");
                why.into_inner()
            },
        };
        let mut logger = guard.borrow_mut();

        logger.log_error(error);
    }

    pub fn init_filesystem(&self, file_manager: FileManagerHandle) {
        let guard = match self.inner.lock() {
            Ok(cell) => cell,
            Err(why) => {
                eprintln!("Logger was poisoned!");
                why.into_inner()
            }
        };

        let mut logger = guard.borrow_mut();
        logger.init_filesystem(file_manager);
    }

    pub async fn flush_logs(&self) {
        let guard = match self.inner.lock() {
            Ok(cell) => cell,
            Err(why) => {
                eprintln!("Logger was poisoned!");
                why.into_inner()
            }
        };

        let mut logger = guard.borrow_mut();
        logger.flush_logs().await;
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum LogLevel {
    Info,
    Warn,
    Error,
}

pub struct Logger {
    logs: Vec<String>,
    counter: LogCounter,
    file_manager: Option<FileManagerHandle>,
}

impl Logger {
    fn new() -> Self {
        Logger {
            logs: Vec::new(),
            counter: LogCounter::new(),
            file_manager: None,
        }
    }

    pub fn log_info(
        &mut self,
        message: &str,
    ) {
        let log_entry = format!(
            "[{:05}] {}\n{:?}\n{}\n----------",
            self.counter.next(),
            Local::now().to_rfc3339(),
            LogLevel::Info,
            message,
        );

        #[cfg(debug_assertions)]
        println!("{log_entry}");

        self.logs.push(log_entry);
    }

    pub fn log_warn(
        &mut self,
        message: &str,
    ) {
        let log_entry = format!(
            "[{:05}] {}\n{:?}\n{}\n----------",
            self.counter.next(),
            Local::now().to_rfc3339(),
            LogLevel::Warn,
            message,
        );

        #[cfg(debug_assertions)]
        println!("{log_entry}");

        self.logs.push(log_entry);
    }

    // HACK: Temporary workaround while restructuring the project.
    pub fn log_error<T>(
        &mut self,
        error: &T,
    ) where
        T: Display + ?Sized,
    {
        let log_entry = format!(
            "[{:05}] {}\n{:?}\n{}\n----------",
            self.counter.next(),
            Local::now().to_rfc3339(),
            LogLevel::Error,
            error,
        );

        #[cfg(debug_assertions)]
        println!("{log_entry}");

        self.logs.push(log_entry);
    }

    pub fn init_filesystem(&mut self, file_manager: FileManagerHandle) {
        self.file_manager = Some(file_manager);
    }

    pub async fn flush_logs(&mut self) {
        let Some(file_manager) = &self.file_manager else { return };

        if self.logs.is_empty() {
            return;
        }

        file_manager.write_server_logs(self.logs.clone()).await;

        self.logs.clear();
    }
}


struct LogCounter {
    date: NaiveDate,
    counter: u32,
}

impl LogCounter {
    fn new() -> Self {
        LogCounter {
            date: Local::now().date_naive(),
            counter: 0,
        }
    }

    fn next(&mut self) -> u32 {
        let today = Local::now().date_naive();

        if today == self.date {
            self.counter += 1;
        } else {
            self.date = today;
            self.counter = 1;
        }

        self.counter
    }
}