use std::{num::NonZeroUsize, sync::Arc};

use anyhow::{Context, Error, Result};
use async_compression::tokio::write::GzipEncoder;
use bytes::Bytes;
use clap::{Parser, ValueEnum};
use data::{DataCLIConfig, generate_batch};
use futures_concurrency::{prelude::ConcurrentStream, stream::StreamExt};
use http::{
    StatusCode,
    header::{ACCEPT, AUTHORIZATION, CONTENT_ENCODING, CONTENT_TYPE},
};
use logging::{LoggingCLIConfig, setup_logging};
use reqwest::Client;
use retry::retry;
use tokio::io::AsyncWriteExt;
use tracing::info;

mod data;
mod logging;
mod retry;

#[derive(Debug, Parser)]
struct Args {
    /// InfluxDB location (schema + hostname, potentially port).
    #[clap(long)]
    url: String,

    /// InfluxDB org.
    #[clap(long)]
    org: String,

    /// InfluxDB bucket.
    #[clap(long)]
    bucket: String,

    /// Auth token.
    #[clap(long)]
    token: String,

    /// Number of batches.
    ///
    /// Defaults to "infinite".
    #[clap(long)]
    batches: Option<usize>,

    /// Concurrency limit.
    #[clap(long, default_value = "4")]
    concurrency_limit: NonZeroUsize,

    /// GZip compression level of HTTP data.
    #[clap(long)]
    compression_level: Option<CompressionLevel>,

    /// Data gen args.
    #[clap(flatten)]
    data_cfg: DataCLIConfig,

    /// Logging args.
    #[clap(flatten)]
    logging_cfg: LoggingCLIConfig,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    setup_logging(args.logging_cfg).context("set up logging")?;

    let client = Client::builder().build().context("build client")?;

    let data_cfg = Arc::new(args.data_cfg);

    futures::stream::iter(0..args.batches.unwrap_or(usize::MAX))
        .co()
        .limit(Some(args.concurrency_limit))
        .map(async |i| {
            let lines = generate_batch(&data_cfg).await.context("generate batch")?;

            let (content_encoding, body) = match args.compression_level {
                None => ("identity", lines.into_bytes()),
                Some(compression_level) => {
                    let mut encoder =
                        GzipEncoder::with_quality(Vec::new(), compression_level.into());
                    encoder
                        .write_all(lines.as_bytes())
                        .await
                        .context("compress data")?;
                    encoder.shutdown().await.context("flush encoder")?;
                    let body = encoder.into_inner();
                    ("gzip", body)
                }
            };

            let body = Bytes::from(body);

            Result::<_, Error>::Ok((i, content_encoding, body))
        })
        .try_for_each(async |res| {
            let (i, content_encoding, body) = res?;

            let request = client
                .post(format!("{}/api/v2/write", args.url.trim_end_matches("/")))
                .query(&[
                    ("org", args.org.as_str()),
                    ("bucket", args.bucket.as_str()),
                    ("precision", "ns"),
                ])
                .header(AUTHORIZATION, format!("Token {}", args.token))
                .header(ACCEPT, "application/json")
                .header(CONTENT_ENCODING, content_encoding)
                .header(CONTENT_TYPE, "text/plain; charset=utf-8")
                .body(body)
                .build()
                .context("build request")?;

            retry(
                "send request",
                async || {
                    let request = request.try_clone().expect("can clone request");
                    let resp = client.execute(request).await?;
                    resp.error_for_status()?;
                    Ok(())
                },
                |err: &reqwest::Error| {
                    err.status()
                        .map(|s| s.is_server_error() || s == StatusCode::TOO_MANY_REQUESTS)
                        .unwrap_or_default()
                },
            )
            .await
            .context("retry request")?;

            info!(batch = i + 1, "sent batch");
            Result::<(), Error>::Ok(())
        })
        .await?;

    Ok(())
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CompressionLevel {
    Fastest,
    Best,
    Default,
}

impl From<CompressionLevel> for async_compression::Level {
    fn from(level: CompressionLevel) -> Self {
        use async_compression::Level;

        match level {
            CompressionLevel::Fastest => Level::Fastest,
            CompressionLevel::Best => Level::Best,
            CompressionLevel::Default => Level::Default,
        }
    }
}
