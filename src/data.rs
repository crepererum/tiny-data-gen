use std::{
    fmt::Write,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Error, Result};
use clap::Parser;
use rand::{Rng, RngCore, distr::Alphabetic};

/// Data generator CLI config.
#[derive(Debug, Parser)]
pub(crate) struct DataCLIConfig {
    /// Number of lines per submission batch.
    #[clap(long, default_value_t = 10_000)]
    batch_lines: usize,
}

pub(crate) async fn generate_batch(config: &Arc<DataCLIConfig>) -> Result<String> {
    let config = Arc::clone(config);
    let lines = tokio::task::spawn_blocking(move || {
        let mut rng = rand::rng();
        let mut lines = String::new();
        for _ in 0..config.batch_lines {
            gen_line(&mut lines, &mut rng).context("gen line")?;
        }
        Result::<_, Error>::Ok(lines)
    })
    .await
    .context("join")??;

    Ok(lines)
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

    writeln!(
        w,
        "table,tag={tag} field_s=\"{field_s}\",field_i={field_i}i,field_u={field_u}u,field_f={field_f},field_b={field_b} {time}"
    )
    .context("write")
}
