use std::{
    fmt::Write,
    num::NonZeroUsize,
    time::{SystemTime, UNIX_EPOCH},
    usize,
};

use anyhow::{Context, Error, Result};
use async_compression::tokio::write::GzipEncoder;
use clap::{Parser, ValueEnum};
use futures_concurrency::{prelude::ConcurrentStream, stream::StreamExt};
use http::header::{ACCEPT, AUTHORIZATION, CONTENT_ENCODING, CONTENT_TYPE};
use rand::{Rng, RngCore, distr::Alphabetic};
use reqwest::Client;
use tokio::io::AsyncWriteExt;

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

    /// Number of lines per submission batch.
    #[clap(long, default_value_t = 10_000)]
    batch_lines: usize,

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
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let client = Client::builder().build().context("build client")?;

    futures::stream::iter(0..args.batches.unwrap_or(usize::MAX))
        .co()
        .limit(Some(args.concurrency_limit))
        .map(async |i| {
            let batch_lines = args.batch_lines;
            let lines = tokio::task::spawn_blocking(move || {
                let mut rng = rand::rng();
                let mut lines = String::new();
                for _ in 0..batch_lines {
                    gen_line(&mut lines, &mut rng).context("gen line")?;
                }
                Result::<_, Error>::Ok(lines)
            })
            .await
            .context("join")??;

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

            Result::<_, Error>::Ok((i, content_encoding, body))
        })
        .try_for_each(async |res| {
            let (i, content_encoding, body) = res?;

            client
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
                .send()
                .await
                .context("post data")?
                .error_for_status()
                .context("error status")?;
            println!("sent {}", i + 1);
            Result::<(), Error>::Ok(())
        })
        .await?;

    Ok(())
}

fn gen_line<W, R>(w: &mut W, rng: &mut R) -> Result<()>
where
    W: Write,
    R: RngCore,
{
    let tag: String = rng
        .sample_iter(Alphabetic)
        .take(1)
        .map(char::from)
        .collect();
    let field_s: String = rng
        .sample_iter(Alphabetic)
        .take(8)
        .map(char::from)
        .collect();
    let field_i: i64 = rng.random();
    let field_u: u64 = rng.random();
    let field_f: f64 = rng.random();
    let field_b: bool = rng.random();
    let time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time should go forward")
        .as_nanos();

    write!(
        w,
        "table,tag={tag} field_s=\"{field_s}\",field_i={field_i}i,field_u={field_u}u,field_f={field_f},field_b={field_b} {time}\n"
    )
    .context("write")
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
