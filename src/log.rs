use colored::{ColoredString, Colorize};
use std::env;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::sync::{LazyLock, Mutex};

static LOG_LEVEL: LazyLock<Level> = LazyLock::new(Level::from_env);
static LOG_FILE: LazyLock<Mutex<Option<BufWriter<File>>>> = LazyLock::new(|| Mutex::new(None));

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Level {
    Trace = 0,
    Debug = 1,
    Info = 2,
    Warn = 3,
    Error = 4,
}

impl Level {
    fn from_env() -> Self {
        dotenv::dotenv().ok();

        if let Ok(path) = env::var("LOG_FILE") {
            init(&path).ok();
        }

        env::var("LOG_LEVEL")
            .ok()
            .and_then(|s| match s.to_lowercase().as_str() {
                "trace" => Some(Level::Trace),
                "debug" => Some(Level::Debug),
                "info" => Some(Level::Info),
                "warn" => Some(Level::Warn),
                "error" => Some(Level::Error),
                _ => None,
            })
            .unwrap_or(Level::Info)
    }
}

#[inline]
fn should_log(level: Level) -> bool {
    level >= *LOG_LEVEL
}

pub fn init(path: &str) -> Result<(), std::io::Error> {
    let stem = path.strip_suffix(".log").unwrap_or(path);
    let timestamped = format!(
        "{}_{}.log",
        stem,
        chrono::Local::now().format("%Y%m%d_%H%M%S")
    );
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&timestamped)?;
    *LOG_FILE.lock().unwrap() = Some(BufWriter::new(file));
    Ok(())
}

#[inline]
pub fn log(level: Level, msg: ColoredString) {
    if !should_log(level) {
        return;
    }

    let now = chrono::Local::now();

    let level_str = match level {
        Level::Trace => "TRACE".dimmed(),
        Level::Debug => "DEBUG".blue().bold(),
        Level::Info => "INFO".green().bold(),
        Level::Warn => "WARN".yellow().bold(),
        Level::Error => "ERROR".red().bold(),
    };

    println!("[{}][{}] {}", now.format("%H:%M:%S"), level_str, msg);

    if let Ok(mut guard) = LOG_FILE.lock()
        && let Some(ref mut file) = *guard
    {
        let level_plain = match level {
            Level::Trace => "TRACE",
            Level::Debug => "DEBUG",
            Level::Info => "INFO",
            Level::Warn => "WARN",
            Level::Error => "ERROR",
        };
        let _ = writeln!(
            file,
            "[{}][{}] {}",
            now.format("%Y-%m-%d %H:%M:%S"),
            level_plain,
            &*msg
        );
        let _ = file.flush();
    }
}

#[macro_export]
macro_rules! trace {
    ($($arg:tt)*) => {
        {
            use colored::Colorize;
            $crate::log::log($crate::log::Level::Trace, format!($($arg)*).dimmed())
        }
    };
}

#[macro_export]
macro_rules! debug {
    ($($arg:tt)*) => {
        {
            use colored::Colorize;
            $crate::log::log($crate::log::Level::Debug, format!($($arg)*).blue())
        }
    };
}

#[macro_export]
macro_rules! info {
    ($($arg:tt)*) => {
        {
            use colored::Colorize;
            $crate::log::log($crate::log::Level::Info, format!($($arg)*).green())
        }
    };
}

#[macro_export]
macro_rules! warn {
    ($($arg:tt)*) => {
        {
            use colored::Colorize;
            $crate::log::log($crate::log::Level::Warn, format!($($arg)*).yellow())
        }
    };
}

#[macro_export]
macro_rules! error {
    ($($arg:tt)*) => {
        {
            use colored::Colorize;
            $crate::log::log($crate::log::Level::Error, format!($($arg)*).red())
        }
    };
}
