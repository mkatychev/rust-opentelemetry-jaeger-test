use std::env;

use argh::FromArgs;
use tracing::Instrument;
use tracing_subscriber::{
    fmt, layer::SubscriberExt, registry, util::SubscriberInitExt, EnvFilter, Layer,
};

#[derive(FromArgs, PartialEq, Debug)]
/// Fetch a URL and export spans.
struct Root {
    /// S3-compatible object storage URL
    #[argh(
        option,
        default = "\"https://sfo3.digitaloceanspaces.com/\".to_owned()"
    )]
    s3_url: String,

    /// jaeger agent endpoint
    #[argh(option)]
    jaeger_agent_endpoint: String,

    /// request backend (reqwest, isahc or surf)
    #[argh(option, default = "\"reqwest\".to_owned()")]
    backend: String,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let root: Root = argh::from_env();

    let subscriber = registry().with(fmt::layer().boxed());
    let subscriber = subscriber
        .with(EnvFilter::new(env::var("RUST_LOG").unwrap_or_else(|_| "error".to_string())).boxed());

    let tracer = opentelemetry_jaeger::new_agent_pipeline()
        .with_endpoint(&root.jaeger_agent_endpoint)
        .with_auto_split_batch(true)
        .with_service_name("opentelemetry-jaeger-test")
        .install_batch(opentelemetry::runtime::Tokio)
        .expect("could not install Jaeger pipeline");

    subscriber
        .with(EnvFilter::new(env::var("RUST_LOG").unwrap_or_else(|_| "error".to_string())).boxed())
        .with(tracing_opentelemetry::layer().with_tracer(tracer).boxed())
        .init();

    let span = tracing::info_span!("main");
    let _guard = span.enter();

    tracing::info!(?root, "Parsed arguments");

    let result = match root.backend.trim().to_lowercase().as_str() {
        "isahc" => {
            make_request_with_isahc(&root.s3_url)
                .in_current_span()
                .await
        }
        "reqwest" => {
            make_request_with_reqwest(&root.s3_url)
                .in_current_span()
                .await
        }
        "surf" => make_request_with_surf(&root.s3_url).in_current_span().await,
        _ => panic!("Unknown backend"),
    };

    tracing::info!(?result, "Finished making request");

    opentelemetry::global::shutdown_tracer_provider();

    tracing::info!("Shut down tracer provider");
}

#[tracing::instrument]
async fn make_request_with_reqwest(url: &str) -> String {
    let response = reqwest::get(url).in_current_span().await.expect("get URL");

    response.text().await.expect("get text")
}

#[tracing::instrument]
async fn make_request_with_isahc(url: &str) -> String {
    use isahc::AsyncReadResponseExt;

    let mut response = isahc::get_async(url).await.expect("get URL");

    response.text().await.expect("get text")
}

#[tracing::instrument]
async fn make_request_with_surf(url: &str) -> String {
    let mut response = surf::get(url).await.expect("get URL");

    response.body_string().await.expect("get text")
}
