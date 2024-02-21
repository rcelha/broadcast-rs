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

impl From<Cli> for ServerConfig {
    fn from(cli: Cli) -> Self {
        ServerConfig {
            backend: BackendConfig::RedisBackend(RedisBrokerConfig {
                redis_url: cli.redis_url,
                channel_capacity: cli.channel_capacity,
            }),
            server_addr: cli.bind,
            server_port: cli.port,
            statsd_host_port: cli.statsd_host.map(|host| (host, cli.statsd_port)),
            otlp_endpoint: cli.otlp_endpoint,
        }
    }
}

#[tokio::main]
async fn main() {
    let args = Cli::parse();
    let server_config: ServerConfig = args.clone().into();
    let app = App::new(server_config);
    app.run().await.expect("Broadcast server failed");
}
