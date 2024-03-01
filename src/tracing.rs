use crate::server::ServerConfig;
use anyhow::Result;
use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use tracing_subscriber::{prelude::*, EnvFilter};

pub fn setup_tracer(config: &ServerConfig) -> Result<()> {
    let stdout_layer = tracing_subscriber::fmt::layer()
        .compact()
        .with_filter(EnvFilter::from_default_env());

    let registry = tracing_subscriber::registry().with(stdout_layer);

    if let Some(endpoint) = &config.otlp_endpoint {
        let tracer = opentelemetry_otlp::new_pipeline()
            .tracing()
            .with_exporter(
                opentelemetry_otlp::new_exporter()
                    .tonic()
                    .with_endpoint(endpoint.clone()),
            )
            .with_trace_config(
                opentelemetry_sdk::trace::config().with_resource(Resource::new(vec![
                    KeyValue::new("service.name", "broadcast"),
                ])),
            )
            .install_batch(opentelemetry_sdk::runtime::Tokio)?;
        let otlp_layer = tracing_opentelemetry::layer().with_tracer(tracer);
        let registry = registry.with(otlp_layer);
        registry.init();
    } else {
        registry.init();
    }
    Ok(())
}
