use clap::Parser;
use proxy_client::cli::Cli;
use proxy_client::config::{metadata_client, Config};
use proxy_client::resource::shared_http_client;
use proxy_client::target::Target;
use proxy_client::worker::{run_worker, WorkerContext};
use proxy_client::{install_rustls_provider, BoxError};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{Level, Metadata};
use tracing_subscriber::filter::FilterExt;
use tracing_subscriber::layer::{Context, Filter, SubscriberExt};
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

const TRANSPORT_LOG_TARGETS: [&str; 7] = [
    "tungstenite",
    "tokio_tungstenite",
    "reqwest",
    "hyper",
    "hyper_util",
    "h2",
    "tower_http",
];

#[derive(Clone, Copy, Debug)]
struct TransportLogCap;

impl<S> Filter<S> for TransportLogCap {
    fn enabled(&self, metadata: &Metadata<'_>, _context: &Context<'_, S>) -> bool {
        transport_level_allowed(metadata.target(), metadata.level())
    }
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let cli = Cli::parse();
    install_rustls_provider();
    let user_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(default_log_filter(cli.verbose)));
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_filter(user_filter.and(TransportLogCap)))
        .try_init()?;

    let _sentry_guard = cli.sentry_dsn.map(|sentry_dsn| {
        sentry::init((
            sentry_dsn,
            sentry::ClientOptions {
                release: sentry::release_name!(),
                attach_stacktrace: true,
                ..Default::default()
            },
        ))
    });

    tracing::info!(config = %cli.config.display(), "loading configuration");
    let config = Arc::new(Config::load(&cli.config, &metadata_client()?).await?);
    let target = Target::parse(&config.target)?;
    let client = shared_http_client()?;
    let context = WorkerContext {
        config,
        target,
        client,
        protocol: cli.protocol,
    };

    let shutdown = CancellationToken::new();
    let mut workers = Vec::with_capacity(cli.parallel);
    for _ in 0..cli.parallel {
        let worker_name = uuid::Uuid::new_v4().simple().to_string();
        workers.push(tokio::spawn(run_worker(
            worker_name,
            context.clone(),
            shutdown.clone(),
        )));
    }

    shutdown_signal().await?;
    shutdown.cancel();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    for worker in workers {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if tokio::time::timeout(remaining, worker).await.is_err() {
            tracing::warn!("graceful shutdown deadline reached; forcing exit");
            break;
        }
    }
    Ok(())
}

fn default_log_filter(verbose: u8) -> &'static str {
    match verbose {
        0 => "warn",
        1 => "info",
        2 => "debug",
        _ => "trace",
    }
}

fn transport_target(target: &str) -> bool {
    TRANSPORT_LOG_TARGETS.iter().any(|prefix| {
        target == *prefix
            || target
                .strip_prefix(prefix)
                .is_some_and(|suffix| suffix.starts_with("::"))
    })
}

fn transport_level_allowed(target: &str, level: &Level) -> bool {
    !transport_target(target) || matches!(*level, Level::WARN | Level::ERROR)
}

async fn shutdown_signal() -> Result<(), BoxError> {
    let ctrl_c = tokio::signal::ctrl_c();

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?
            .recv()
            .await;
        Ok::<(), std::io::Error>(())
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<Result<(), std::io::Error>>();

    tokio::select! {
        result = ctrl_c => result?,
        result = terminate => result?,
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{default_log_filter, transport_level_allowed, transport_target, TransportLogCap};
    use std::sync::{Arc, Mutex};
    use tracing::{Event, Level, Subscriber};
    use tracing_subscriber::filter::FilterExt;
    use tracing_subscriber::layer::{Context, SubscriberExt};
    use tracing_subscriber::{EnvFilter, Layer, Registry};

    #[derive(Clone)]
    struct RecordingLayer(Arc<Mutex<Vec<(String, Level)>>>);

    impl<S: Subscriber> Layer<S> for RecordingLayer {
        fn on_event(&self, event: &Event<'_>, _context: Context<'_, S>) {
            self.0.lock().unwrap().push((
                event.metadata().target().to_owned(),
                *event.metadata().level(),
            ));
        }
    }

    #[test]
    fn verbose_count_maps_to_a_default_filter() {
        assert_eq!(default_log_filter(0), "warn");
        assert_eq!(default_log_filter(1), "info");
        assert_eq!(default_log_filter(2), "debug");
        assert_eq!(default_log_filter(3), "trace");
    }

    #[test]
    fn matches_only_header_carrying_transport_target_prefixes() {
        for target in [
            "tungstenite",
            "tungstenite::handshake::client",
            "tokio_tungstenite::connect",
            "reqwest::connect",
            "hyper::client",
            "hyper_util::client",
            "h2::codec",
            "tower_http::trace",
        ] {
            assert!(transport_target(target), "target was not capped: {target}");
        }
        for target in ["proxy_client", "hyperactive", "reqwestish", "tower"] {
            assert!(
                !transport_target(target),
                "target was over-capped: {target}"
            );
        }
    }

    #[test]
    fn transport_policy_caps_debug_and_trace_but_keeps_app_trace() {
        for target in ["tungstenite", "reqwest::connect", "hyper::client"] {
            assert!(!transport_level_allowed(target, &Level::TRACE));
            assert!(!transport_level_allowed(target, &Level::DEBUG));
            assert!(transport_level_allowed(target, &Level::WARN));
            assert!(transport_level_allowed(target, &Level::ERROR));
        }
        assert!(transport_level_allowed(
            "proxy_client::worker",
            &Level::TRACE
        ));
    }

    #[test]
    fn global_trace_and_hard_cap_are_composed_with_and() {
        let recorded = Arc::new(Mutex::new(Vec::new()));
        let subscriber = Registry::default().with(
            RecordingLayer(recorded.clone())
                .with_filter(EnvFilter::new("trace").and(TransportLogCap)),
        );

        tracing::subscriber::with_default(subscriber, || {
            tracing::event!(
                target: "tungstenite::handshake::client",
                Level::TRACE,
                "raw request headers"
            );
            tracing::event!(
                target: "tungstenite::handshake::client",
                Level::WARN,
                "transport warning"
            );
            tracing::event!(
                target: "proxy_client::worker",
                Level::TRACE,
                "application trace"
            );
        });

        assert_eq!(
            *recorded.lock().unwrap(),
            vec![
                ("tungstenite::handshake::client".to_owned(), Level::WARN),
                ("proxy_client::worker".to_owned(), Level::TRACE),
            ]
        );
    }
}
