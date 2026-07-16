use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "sumi_agent=info".into()),
        )
        .init();

    tracing::info!("sumi-agent starting");

    tokio::signal::ctrl_c().await?;

    tracing::info!("sumi-agent shutting down");
    Ok(())
}
