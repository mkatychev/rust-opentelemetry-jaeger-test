use std::env;

use argh::FromArgs;
use opentelemetry::{
    trace::{FutureExt, TraceContextExt},
    Context,
};
use tracing::{Instrument, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::{
    fmt::{self, format::FmtSpan},
    layer::SubscriberExt,
    registry,
    util::SubscriberInitExt,
    EnvFilter, Layer,
};

#[derive(FromArgs, PartialEq, Debug)]
/// Fetch a URL and export spans.
struct Root {
    /// URL to fetch
    #[argh(option, default = "\"http://www.google.com/\".to_owned()")]
    url: String,

    /// jaeger agent endpoint
    #[argh(option)]
    jaeger_agent_endpoint: String,

    /// request backend (reqwest, isahc or surf)
    #[argh(option, default = "\"reqwest\".to_owned()")]
    backend: String,

    /// produce JSON output including span informaton
    #[argh(switch)]
    json: bool,

    /// logging configuration for tracing_subscriber::EnvFilter
    #[argh(
        option,
        default = "env::var(\"RUST_LOG\").unwrap_or_else(|_| \"debug,rust_opentelemetry_jaeger_test=trace\".to_string())"
    )]
    log: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let root: Root = argh::from_env();

    let subscriber = if root.json {
        // TODO add trace ID here when possible: https://github.com/tokio-rs/tracing/discussions/1703, https://github.com/tokio-rs/tracing/issues/1481, https://github.com/tokio-rs/tracing/issues/1531
        registry().with(
            fmt::layer()
                .json()
                .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
                .with_span_list(false)
                .boxed(),
        )
    } else {
        registry().with(fmt::layer().without_time().with_target(false).boxed())
    };

    let subscriber = subscriber.with(EnvFilter::new(&root.log).boxed());

    let tracer = opentelemetry_jaeger::new_agent_pipeline()
        .with_endpoint(&root.jaeger_agent_endpoint)
        .with_auto_split_batch(true)
        .with_service_name("opentelemetry-jaeger-test")
        .install_batch(opentelemetry::runtime::TokioCurrentThread)
        .expect("could not install Jaeger pipeline");

    subscriber
        .with(tracing_opentelemetry::layer().with_tracer(tracer).boxed())
        .init();

    tracing::info!(?root, "Parsed arguments");

    {
        let span = tracing::info_span!("main");
        tracing::warn!(span_span_id = ?span.context().span().span_context().span_id(), "pre_run");
        tracing::warn!(cx_span_id = ?Context::current().span().span_context().span_id(), "pre_run");
        run(&root)
            .with_context(span.context())
            .instrument(span)
            .await;
    }

    opentelemetry::global::shutdown_tracer_provider();

    tracing::info!("Shut down tracer provider");
}

#[tracing::instrument(skip_all)]
async fn run(root: &Root) {
    tracing::warn!(span_span_id = ?Span::current().context().span().span_context().span_id());
    tracing::warn!(cx_span_id = ?Context::current().span().span_context().span_id());
    let _result = match root.backend.trim().to_lowercase().as_str() {
        "isahc" => {
            make_request_with_isahc(&root.url)
                .with_current_context()
                .in_current_span()
                .await
        }
        "reqwest" => {
            make_request_with_reqwest(&root.url)
                .with_current_context()
                .in_current_span()
                .await
        }
        "surf" => {
            make_request_with_surf(&root.url)
                .with_context(Span::current().context())
                .await
        }
        _ => panic!("Unknown backend"),
    };

    tracing::info!("Finished making request");
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

#[tracing::instrument(skip_all)]
async fn make_request_with_surf(url: &str) -> String {
    tracing::warn!(span_span_id = ?Span::current().context().span().span_context().span_id(), "make_request");
    tracing::warn!(cx_span_id = ?Context::current().span().span_context().span_id(), "make_request");
    let mut response = surf::get(url).await.expect("get URL");

    response.body_string().await.expect("get text")
}
