//! Logging setup.
use std::io::IsTerminal;

use anyhow::{Context, Result};
use clap::Parser;
use tracing_log::LogTracer;
use tracing_subscriber::{EnvFilter, FmtSubscriber};

/// Logging CLI config.
#[derive(Debug, Parser)]
pub(crate) struct LoggingCLIConfig {
    /// Log filter.
    ///
    /// Conflicts with `-v`/`--verbose`.
    #[clap(
        conflicts_with = "log_verbose_count",
        long,
        env = "COMMON_SENSE_CODER_LOGGING_FILTER"
    )]
    log_filter: Option<String>,

    /// Verbose logs.
    ///
    /// Repeat to increase verbosity.
    ///
    /// Conflicts with `--log-filter`.
    #[clap(
        short = 'v',
        long = "verbose",
        conflicts_with="log_filter",
        action = clap::ArgAction::Count,
    )]
    log_verbose_count: u8,
}

/// Setup process-wide logging.
pub(crate) fn setup_logging(config: LoggingCLIConfig) -> Result<()> {
    LogTracer::init().context("init log tracer")?;

    let filter = match config.log_filter {
        Some(filter) => filter,
        None => match config.log_verbose_count {
            0 => "warn".to_owned(),
            1 => "info".to_owned(),
            2 => format!("info,{}=debug", env!("CARGO_CRATE_NAME")),
            3 => "debug".to_owned(),
            _ => "trace".to_owned(),
        },
    };
    let filter = EnvFilter::try_new(filter).context("set up log env filter")?;

    let writer = std::io::stdout;
    let subscriber = FmtSubscriber::builder()
        .with_ansi(writer().is_terminal())
        .with_env_filter(filter)
        .with_writer(writer)
        .finish();

    tracing::subscriber::set_global_default(subscriber).context("install tracinng subscriber")?;

    Ok(())
}
