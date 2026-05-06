use clap::Parser;
use rts_wiki_monitor::{Result, cli::Cli};

#[tokio::main]
async fn main() -> Result<()> {
    Cli::parse().run().await
}
