use broadcast_rs::prelude::*;
use clap::Parser;

#[derive(Parser, Debug, Clone)]
struct Cli {
    #[clap(short, long, default_value = "redis://localhost:6666")]
    redis_url: String,
    #[clap(short, long, default_value = "100")]
    channel_capacity: usize,
    #[clap(short, long, default_value = "0.0.0.0")]
    bind: String,
    #[clap(short, long, default_value = "3000")]
    port: u16,
    #[clap(long, default_value = None)]
    statsd_host: Option<String>,
    #[clap(long, default_value = "8125")]
    statsd_port: u16,
    #[clap(long, default_value = None)]
    otlp_endpoint: Option<String>,
}

impl Into<ServerConfig> for Cli {
    fn into(self) -> ServerConfig {
        ServerConfig {
            redis_url: self.redis_url,
            channel_capacity: self.channel_capacity,
            server_addr: self.bind,
            server_port: self.port,
            statsd_host_port: self.statsd_host.map(|host| (host, self.statsd_port)),
            otlp_endpoint: self.otlp_endpoint,
        }
    }
}

#[tokio::main]
async fn main() {
    let args = Cli::parse();
    let server_config: ServerConfig = args.clone().into();
    start_server(server_config)
        .await
        .expect("Broadcast server failed");
}
