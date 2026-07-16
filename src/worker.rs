use crate::config::Config;
use crate::memory::{BudgetedResponse, MemoryBudget, MemoryReservation};
use crate::protocol::{
    ready_json, register_json, response_form_encoded_len, response_json, response_json_encoded_len,
    IncomingMessage, ResourceResponse, PING_JSON, PONG_JSON,
};
use crate::resource::call_resource;
use crate::target::Target;
use crate::{BoxError, MAX_WS_MESSAGE_SIZE};
use futures_util::stream::FuturesUnordered;
use futures_util::{Sink, SinkExt, StreamExt};
use reqwest::header::{HeaderValue as ReqwestHeaderValue, CONTENT_TYPE};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_tungstenite::connect_async_with_config;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::protocol::{Message, WebSocketConfig};
use tokio_util::sync::CancellationToken;

const CF_ACCESS_CLIENT_ID: &str = "CF-Access-Client-Id";
const CF_ACCESS_CLIENT_SECRET: &str = "CF-Access-Client-Secret";
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(20);
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(45);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const RECONNECT_DELAY: Duration = Duration::from_secs(1);
const BUSY_RESPONSE_QUEUE_CAPACITY: usize = 8;
const BUSY_RESPONSE_CONCURRENCY: usize = 4;
const BUSY_POST_TIMEOUT: Duration = Duration::from_secs(5);
#[cfg(not(test))]
const WS_SEND_TIMEOUT: Duration = Duration::from_secs(5);
#[cfg(test)]
const WS_SEND_TIMEOUT: Duration = Duration::from_millis(100);

#[derive(Clone)]
pub struct WorkerContext {
    pub config: Arc<Config>,
    pub target: Target,
    pub client: Arc<reqwest::Client>,
    pub protocol: u8,
    pub memory_budget: MemoryBudget,
}

struct Completion {
    response: Option<BudgetedResponse>,
    delivered_by_http: bool,
}

pub async fn run_worker(worker_name: String, context: WorkerContext, shutdown: CancellationToken) {
    tracing::info!(worker = %worker_name, "worker started");
    while !shutdown.is_cancelled() {
        if let Err(error) = run_connection(&worker_name, &context, &shutdown).await {
            if !shutdown.is_cancelled() {
                tracing::warn!(worker = %worker_name, error = %error, "connection ended");
            }
        }
        tokio::select! {
            () = shutdown.cancelled() => break,
            () = tokio::time::sleep(RECONNECT_DELAY) => {}
        }
    }
    tracing::info!(worker = %worker_name, "worker stopped");
}

pub async fn run_connection(
    worker_name: &str,
    context: &WorkerContext,
    shutdown: &CancellationToken,
) -> Result<(), BoxError> {
    crate::protocol::validate_instance(&context.config.instance)?;
    crate::protocol::validate_worker(worker_name)?;
    let mut request = context.target.ws_url().as_str().into_client_request()?;
    add_websocket_cf_headers(request.headers_mut(), &context.config)?;

    let mut websocket_config = WebSocketConfig::default();
    websocket_config.max_message_size = Some(MAX_WS_MESSAGE_SIZE);
    websocket_config.max_frame_size = Some(MAX_WS_MESSAGE_SIZE);
    websocket_config.max_write_buffer_size = MAX_WS_MESSAGE_SIZE + 1024 * 1024;
    let (websocket, _) = tokio::time::timeout(
        CONNECT_TIMEOUT,
        connect_async_with_config(request, Some(websocket_config), false),
    )
    .await??;
    let (mut write, mut read) = websocket.split();

    send_ws(
        &mut write,
        Message::Text(
            register_json(context.protocol, &context.config.instance, worker_name)?.into(),
        ),
    )
    .await?;
    tracing::info!(worker = %worker_name, "connected and registered");

    let (completion_tx, mut completion_rx) = mpsc::channel::<Completion>(1);
    let (busy_response_tx, busy_response_rx) =
        mpsc::channel::<BudgetedResponse>(BUSY_RESPONSE_QUEUE_CAPACITY);
    let (busy_fallback_tx, mut busy_fallback_rx) =
        mpsc::channel::<BudgetedResponse>(BUSY_RESPONSE_QUEUE_CAPACITY);
    let busy_response_actor = tokio::spawn(run_busy_response_actor(
        context.clone(),
        busy_response_rx,
        busy_fallback_tx,
    ));
    let mut active_task: Option<JoinHandle<()>> = None;
    let mut heartbeat = tokio::time::interval(HEARTBEAT_INTERVAL);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    heartbeat.tick().await;
    let mut last_pong = Instant::now();

    let result = async {
        loop {
            tokio::select! {
            () = shutdown.cancelled() => {
                let _ = send_ws(&mut write, Message::Close(None)).await;
                break Ok(());
            }
            _ = heartbeat.tick() => {
                if last_pong.elapsed() > HEARTBEAT_TIMEOUT {
                    break Err("websocket heartbeat timed out".into());
                }
                send_ws(&mut write, Message::Text(PING_JSON.into())).await?;
                send_ws(&mut write, Message::Ping(worker_name.as_bytes().to_vec().into())).await?;
            }
            completion = completion_rx.recv(), if active_task.is_some() => {
                let Some(completion) = completion else {
                    break Err("resource task channel closed".into());
                };
                if let Some(task) = active_task.take() {
                    task.await?;
                }
                if !completion.delivered_by_http {
                    if let Some(response) = completion.response {
                        send_response_ws(&mut write, &context.memory_budget, response).await?;
                    }
                }
            }
            fallback = busy_fallback_rx.recv() => {
                let Some(fallback) = fallback else {
                    break Err("busy response actor closed unexpectedly".into());
                };
                send_response_ws(&mut write, &context.memory_budget, fallback).await?;
            }
            message = read.next() => {
                let Some(message) = message else {
                    break Err("websocket closed without a close frame".into());
                };
                match message? {
                    Message::Text(text) => {
                        let incoming = match serde_json::from_str::<IncomingMessage>(text.as_str()) {
                            Ok(incoming) => incoming,
                            Err(error) => {
                                tracing::warn!(error = %error, "ignored invalid websocket JSON");
                                continue;
                            }
                        };
                        if let Err(error) = incoming.validate() {
                            tracing::warn!(error = %error, "ignored websocket message with oversized identifier");
                            continue;
                        }
                        match incoming {
                            IncomingMessage::Ping => {
                                send_ws(&mut write, Message::Text(PONG_JSON.into())).await?;
                            }
                            IncomingMessage::Pong => last_pong = Instant::now(),
                            IncomingMessage::Ready { uid } => {
                                if context.protocol >= 2 && active_task.is_none() {
                                    send_ws(
                                        &mut write,
                                        Message::Text(ready_json(&uid, worker_name)?.into()),
                                    )
                                    .await?;
                                } else {
                                    tracing::warn!("ignored ready message while worker is busy");
                                }
                            }
                            IncomingMessage::Request { uid, resource } => {
                                if active_task.is_some() {
                                    tracing::warn!("rejected a second request while worker is busy");
                                    let busy = match BudgetedResponse::try_new(
                                        &context.memory_budget,
                                        uid,
                                        503,
                                        String::new(),
                                    ) {
                                        Ok(busy) => busy,
                                        Err(error) => {
                                            tracing::warn!(error = %error, "busy response rejected by memory budget");
                                            continue;
                                        }
                                    };
                                    if context.protocol == 3 {
                                        if let Err(error) = busy_response_tx.try_send(busy) {
                                            let busy = error.into_inner();
                                            send_response_ws(
                                                &mut write,
                                                &context.memory_budget,
                                                busy,
                                            )
                                            .await?;
                                        }
                                    } else {
                                        send_response_ws(
                                            &mut write,
                                            &context.memory_budget,
                                            busy,
                                        )
                                        .await?;
                                    }
                                    continue;
                                }

                                let task_context = context.clone();
                                let task_completion_tx = completion_tx.clone();
                                active_task = Some(tokio::spawn(async move {
                                    let response = call_resource(
                                        &task_context.client,
                                        &task_context.config,
                                        uid,
                                        &resource,
                                        &task_context.memory_budget,
                                    )
                                    .await;
                                    let delivered_by_http = if task_context.protocol == 3 {
                                        match response.as_ref() {
                                            Some(response) => match post_response(&task_context, response).await {
                                                Ok(()) => true,
                                                Err(error) => {
                                                    tracing::warn!(error = %error, "unable to deliver v3 response; falling back to websocket");
                                                    false
                                                }
                                            }
                                            None => false,
                                        }
                                    } else {
                                        false
                                    };
                                    let _ = task_completion_tx
                                        .send(Completion {
                                            response,
                                            delivered_by_http,
                                        })
                                        .await;
                                }));
                            }
                            IncomingMessage::Unknown => tracing::warn!("ignored unknown websocket message"),
                        }
                    }
                    Message::Ping(payload) => {
                        send_ws(&mut write, Message::Pong(payload)).await?;
                    }
                    Message::Pong(_) => last_pong = Instant::now(),
                    Message::Close(frame) => {
                        let _ = send_ws(&mut write, Message::Close(frame)).await;
                        break Ok(());
                    }
                    Message::Binary(_) | Message::Frame(_) => {
                        tracing::warn!("ignored unsupported websocket message");
                    }
                }
            }
            }
        }
    }
    .await;

    if let Some(task) = active_task {
        task.abort();
        let _ = task.await;
    }
    busy_response_actor.abort();
    let _ = busy_response_actor.await;
    result
}

async fn run_busy_response_actor(
    context: WorkerContext,
    mut responses: mpsc::Receiver<BudgetedResponse>,
    fallbacks: mpsc::Sender<BudgetedResponse>,
) -> Result<(), BoxError> {
    let mut posts = FuturesUnordered::new();
    let mut input_open = true;

    loop {
        if !input_open && posts.is_empty() {
            return Ok(());
        }

        tokio::select! {
            response = responses.recv(), if input_open && posts.len() < BUSY_RESPONSE_CONCURRENCY => {
                match response {
                    Some(response) => {
                        let post_context = context.clone();
                        posts.push(async move {
                            let result = match tokio::time::timeout(
                                BUSY_POST_TIMEOUT,
                                post_response(&post_context, &response),
                            )
                            .await
                            {
                                Ok(result) => result,
                                Err(error) => Err(Box::new(error) as BoxError),
                            };
                            (response, result)
                        });
                    }
                    None => input_open = false,
                }
            }
            completed = posts.next(), if !posts.is_empty() => {
                let Some((response, result)) = completed else {
                    continue;
                };
                if let Err(error) = result {
                    tracing::warn!(
                        uid = response.uid,
                        error = %error,
                        "unable to deliver busy v3 response; falling back to websocket"
                    );
                    fallbacks.send(response).await?;
                }
            }
        }
    }
}

async fn send_ws<S>(sink: &mut S, message: Message) -> Result<(), BoxError>
where
    S: Sink<Message> + Unpin,
    S::Error: std::error::Error + Send + Sync + 'static,
{
    tokio::time::timeout(WS_SEND_TIMEOUT, sink.send(message))
        .await?
        .map_err(|error| Box::new(error) as BoxError)
}

async fn send_response_ws<S>(
    sink: &mut S,
    memory_budget: &MemoryBudget,
    mut response: BudgetedResponse,
) -> Result<(), BoxError>
where
    S: Sink<Message> + Unpin,
    S::Error: std::error::Error + Send + Sync + 'static,
{
    let encoded = bounded_response_json(&mut response, memory_budget)?;
    let EncodedResponse { json, reservation } = encoded;
    let result = send_ws(sink, Message::Text(json.into())).await;
    drop(reservation);
    result
}

struct EncodedResponse {
    json: String,
    reservation: MemoryReservation,
}

fn bounded_response_json(
    response: &mut BudgetedResponse,
    memory_budget: &MemoryBudget,
) -> Result<EncodedResponse, BoxError> {
    bounded_response_json_with_limit_and_encoder(
        response,
        memory_budget,
        MAX_WS_MESSAGE_SIZE,
        response_json,
    )
}

fn bounded_response_json_with_limit_and_encoder<F>(
    response: &mut BudgetedResponse,
    memory_budget: &MemoryBudget,
    limit: usize,
    encoder: F,
) -> Result<EncodedResponse, BoxError>
where
    F: FnOnce(&ResourceResponse) -> serde_json::Result<String>,
{
    let original_length = response_json_encoded_len(response)
        .ok_or("websocket response length calculation overflowed")?;
    let reservation = if original_length <= limit {
        memory_budget.try_reserve(original_length).ok()
    } else {
        None
    };

    let (encoded_length, mut reservation) = match reservation {
        Some(reservation) => (original_length, reservation),
        None => {
            response.downgrade_to_error();
            let fallback_length = response_json_encoded_len(response)
                .ok_or("websocket fallback length calculation overflowed")?;
            if fallback_length > limit {
                return Err("websocket response metadata exceeds the message limit".into());
            }
            let reservation = memory_budget.try_reserve(fallback_length)?;
            (fallback_length, reservation)
        }
    };

    let json = encoder(response)?;
    if json.len() != encoded_length {
        return Err("websocket response length precheck disagreed with serializer".into());
    }
    if json.capacity() > encoded_length {
        reservation.try_grow(json.capacity() - encoded_length)?;
    }
    Ok(EncodedResponse { json, reservation })
}

fn add_websocket_cf_headers(
    headers: &mut tokio_tungstenite::tungstenite::http::HeaderMap,
    config: &Config,
) -> Result<(), BoxError> {
    if config.cf_access_enabled {
        headers.insert(
            CF_ACCESS_CLIENT_ID,
            HeaderValue::from_str(&config.cf_access_key)?,
        );
        headers.insert(
            CF_ACCESS_CLIENT_SECRET,
            HeaderValue::from_str(&config.cf_access_secret)?,
        );
    }
    Ok(())
}

async fn post_response(
    context: &WorkerContext,
    response: &BudgetedResponse,
) -> Result<(), BoxError> {
    let prepared = prepare_form_request(context, response)?;
    let PreparedFormRequest {
        request,
        reservation,
    } = prepared;
    let result = request.send().await?.error_for_status().map(|_| ());
    drop(reservation);
    result.map_err(|error| Box::new(error) as BoxError)
}

struct PreparedFormRequest {
    request: reqwest::RequestBuilder,
    reservation: MemoryReservation,
}

fn prepare_form_request(
    context: &WorkerContext,
    response: &BudgetedResponse,
) -> Result<PreparedFormRequest, BoxError> {
    prepare_form_request_with_encoder(context, response, encode_form)
}

fn prepare_form_request_with_encoder<F>(
    context: &WorkerContext,
    response: &BudgetedResponse,
    encoder: F,
) -> Result<PreparedFormRequest, BoxError>
where
    F: FnOnce(&ResourceResponse, usize) -> Result<String, BoxError>,
{
    let encoded_length =
        response_form_encoded_len(response).ok_or("form response length calculation overflowed")?;
    let mut reservation = context.memory_budget.try_reserve(encoded_length)?;
    let form = encoder(response, encoded_length)?;
    if form.len() != encoded_length {
        return Err("form response length precheck disagreed with serializer".into());
    }
    if form.capacity() > encoded_length {
        reservation.try_grow(form.capacity() - encoded_length)?;
    }
    let url = context.target.response_url(&response.uid)?;
    let mut request = context
        .client
        .post(url)
        .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
        .body(form);
    if context.config.cf_access_enabled {
        request = request
            .header(
                CF_ACCESS_CLIENT_ID,
                ReqwestHeaderValue::from_str(&context.config.cf_access_key)?,
            )
            .header(
                CF_ACCESS_CLIENT_SECRET,
                ReqwestHeaderValue::from_str(&context.config.cf_access_secret)?,
            );
    }
    Ok(PreparedFormRequest {
        request,
        reservation,
    })
}

fn encode_form(response: &ResourceResponse, encoded_length: usize) -> Result<String, BoxError> {
    let status = response.status.to_string();
    let mut serializer =
        url::form_urlencoded::Serializer::new(String::with_capacity(encoded_length));
    serializer.append_pair("status", &status);
    serializer.append_pair("body", &response.body);
    Ok(serializer.finish())
}

#[cfg(test)]
mod tests {
    use super::{run_connection, WorkerContext, CF_ACCESS_CLIENT_ID, CF_ACCESS_CLIENT_SECRET};
    use crate::config::Config;
    use crate::memory::{BudgetedResponse, MemoryBudget};
    use crate::target::Target;
    use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
    use axum::extract::{Form, Path, State};
    use axum::http::HeaderMap;
    use axum::response::Response;
    use axum::routing::{any, get, post};
    use axum::Router;
    use futures_util::StreamExt;
    use serde_json::{json, Value};
    use std::cell::{Cell, RefCell};
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    #[derive(Debug)]
    enum Event {
        Register(Value),
        Ready(Value),
        Pong,
        WsResponse(Value),
        HttpResponse {
            uid: String,
            form: HashMap<String, String>,
            headers: HeaderMap,
        },
        WsHeaders(HeaderMap),
    }

    #[derive(Clone)]
    struct TestState {
        protocol: u8,
        fail_post: bool,
        post_delay: std::time::Duration,
        request_uids: Arc<Vec<String>>,
        exporter_stats: ExporterStats,
        events: mpsc::Sender<Event>,
    }

    #[derive(Clone, Default)]
    struct ExporterStats {
        active: Arc<AtomicUsize>,
        calls: Arc<AtomicUsize>,
        max_active: Arc<AtomicUsize>,
    }

    async fn ws_handler(
        State(state): State<TestState>,
        headers: HeaderMap,
        upgrade: WebSocketUpgrade,
    ) -> Response {
        let _ = state.events.send(Event::WsHeaders(headers)).await;
        upgrade.on_upgrade(move |socket| server_actor(socket, state))
    }

    async fn server_actor(mut socket: WebSocket, state: TestState) {
        let register = next_json(&mut socket).await;
        let _ = state.events.send(Event::Register(register)).await;
        if state.protocol >= 2 {
            socket
                .send(Message::Text(
                    json!({"type":"ready", "uid":"ready-1"}).to_string().into(),
                ))
                .await
                .unwrap();
            let ready = next_json(&mut socket).await;
            let _ = state.events.send(Event::Ready(ready)).await;
        }
        for uid in state.request_uids.iter() {
            socket
                .send(Message::Text(
                    json!({"type":"request", "uid":uid, "resource":"node"})
                        .to_string()
                        .into(),
                ))
                .await
                .unwrap();
        }
        socket
            .send(Message::Text(json!({"type":"ping"}).to_string().into()))
            .await
            .unwrap();

        while let Some(Ok(message)) = socket.next().await {
            match message {
                Message::Text(text) => {
                    let value: Value = serde_json::from_str(text.as_str()).unwrap();
                    match value.get("type").and_then(Value::as_str) {
                        Some("pong") => {
                            let _ = state.events.send(Event::Pong).await;
                        }
                        Some("response") => {
                            let _ = state.events.send(Event::WsResponse(value)).await;
                        }
                        _ => {}
                    }
                }
                Message::Ping(payload) => {
                    let _ = socket.send(Message::Pong(payload)).await;
                }
                Message::Close(_) => break,
                _ => {}
            }
        }
    }

    async fn next_json(socket: &mut WebSocket) -> Value {
        loop {
            match socket.next().await.unwrap().unwrap() {
                Message::Text(text) => return serde_json::from_str(text.as_str()).unwrap(),
                Message::Ping(payload) => socket.send(Message::Pong(payload)).await.unwrap(),
                _ => {}
            }
        }
    }

    async fn response_handler(
        State(state): State<TestState>,
        Path(uid): Path<String>,
        headers: HeaderMap,
        Form(form): Form<HashMap<String, String>>,
    ) -> axum::http::StatusCode {
        tokio::time::sleep(state.post_delay).await;
        let _ = state
            .events
            .send(Event::HttpResponse { uid, form, headers })
            .await;
        if state.fail_post {
            axum::http::StatusCode::INTERNAL_SERVER_ERROR
        } else {
            axum::http::StatusCode::OK
        }
    }

    async fn exporter_handler(State(state): State<TestState>) -> &'static str {
        state.exporter_stats.calls.fetch_add(1, Ordering::SeqCst);
        let active = state.exporter_stats.active.fetch_add(1, Ordering::SeqCst) + 1;
        state
            .exporter_stats
            .max_active
            .fetch_max(active, Ordering::SeqCst);
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        state.exporter_stats.active.fetch_sub(1, Ordering::SeqCst);
        "metric 1\n"
    }

    async fn start_server_with_requests(
        protocol: u8,
        fail_post: bool,
        request_uids: &[&str],
    ) -> (String, mpsc::Receiver<Event>, ExporterStats) {
        start_server_with_requests_and_delay(
            protocol,
            fail_post,
            request_uids,
            std::time::Duration::ZERO,
        )
        .await
    }

    async fn start_server_with_requests_and_delay(
        protocol: u8,
        fail_post: bool,
        request_uids: &[&str],
        post_delay: std::time::Duration,
    ) -> (String, mpsc::Receiver<Event>, ExporterStats) {
        let (events, receiver) = mpsc::channel(32);
        let exporter_stats = ExporterStats::default();
        let state = TestState {
            protocol,
            fail_post,
            post_delay,
            request_uids: Arc::new(request_uids.iter().map(ToString::to_string).collect()),
            exporter_stats: exporter_stats.clone(),
            events,
        };
        let app = Router::new()
            .route("/proxy/ws/", any(ws_handler))
            .route("/proxy/response/{uid}/", post(response_handler))
            .route("/metrics", get(exporter_handler))
            .with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{address}"), receiver, exporter_stats)
    }

    async fn start_server(protocol: u8, fail_post: bool) -> (String, mpsc::Receiver<Event>) {
        let (base, events, _) =
            start_server_with_requests(protocol, fail_post, &["request-1"]).await;
        (base, events)
    }

    fn context(base: &str, protocol: u8, cf: bool) -> WorkerContext {
        crate::install_rustls_provider();
        WorkerContext {
            config: Arc::new(Config {
                instance: "instance-1".into(),
                target: format!("{base}/proxy/"),
                resources: HashMap::from([("node".into(), format!("{base}/metrics"))]),
                cf_access_enabled: cf,
                cf_access_key: "client-id".into(),
                cf_access_secret: "client-secret".into(),
                ec2_meta_domain: String::new(),
            }),
            target: Target::parse(&format!("{base}/proxy/")).unwrap(),
            client: Arc::new(reqwest::Client::new()),
            protocol,
            memory_budget: MemoryBudget::new(crate::memory::DEFAULT_MEMORY_BUDGET_SIZE),
        }
    }

    #[tokio::test]
    async fn v2_roundtrip_keeps_heartbeats_responsive_during_exporter_call() {
        let (base, mut events) = start_server(2, false).await;
        let cancellation = CancellationToken::new();
        let task = tokio::spawn({
            let context = context(&base, 2, false);
            let cancellation = cancellation.clone();
            async move { run_connection("worker-1", &context, &cancellation).await }
        });

        assert_eq!(
            take_register(&mut events).await,
            json!({"type":"register", "instance":"instance-1", "worker":"worker-1", "version":2})
        );
        assert_eq!(
            take_ready(&mut events).await,
            json!({"type":"ready", "uid":"ready-1", "worker":"worker-1"})
        );
        assert!(matches!(events.recv().await.unwrap(), Event::Pong));
        let response = match events.recv().await.unwrap() {
            Event::WsResponse(value) => value,
            event => panic!("unexpected event: {event:?}"),
        };
        assert_eq!(response["status"], 200);
        assert_eq!(response["body"], "metric 1\n");
        cancellation.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn v1_roundtrip_registers_without_worker_and_handles_heartbeat() {
        let (base, mut events) = start_server(1, false).await;
        let cancellation = CancellationToken::new();
        let task = tokio::spawn({
            let context = context(&base, 1, false);
            let cancellation = cancellation.clone();
            async move { run_connection("unused-worker", &context, &cancellation).await }
        });

        assert_eq!(
            take_register(&mut events).await,
            json!({"type":"register", "instance":"instance-1"})
        );
        let mut pong_seen = false;
        let response = loop {
            match recv_event(&mut events).await {
                Event::Pong => pong_seen = true,
                Event::WsResponse(response) => break response,
                event => panic!("unexpected event: {event:?}"),
            }
        };
        assert!(pong_seen);
        assert_eq!(response["uid"], "request-1");
        assert_eq!(response["status"], 200);
        assert_eq!(response["body"], "metric 1\n");
        cancellation.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn v3_posts_form_and_cf_headers_on_both_transports() {
        let (base, mut events) = start_server(3, false).await;
        let cancellation = CancellationToken::new();
        let task = tokio::spawn({
            let context = context(&base, 3, true);
            let cancellation = cancellation.clone();
            async move { run_connection("worker-3", &context, &cancellation).await }
        });

        let ws_headers = match events.recv().await.unwrap() {
            Event::WsHeaders(headers) => headers,
            event => panic!("unexpected event: {event:?}"),
        };
        assert_eq!(ws_headers[CF_ACCESS_CLIENT_ID], "client-id");
        assert_eq!(ws_headers[CF_ACCESS_CLIENT_SECRET], "client-secret");
        assert_eq!(
            take_register(&mut events).await,
            json!({"type":"register", "instance":"instance-1", "worker":"worker-3", "version":3})
        );
        let _ = take_ready(&mut events).await;
        assert!(matches!(events.recv().await.unwrap(), Event::Pong));
        let (uid, form, headers) = loop {
            match events.recv().await.unwrap() {
                Event::HttpResponse { uid, form, headers } => break (uid, form, headers),
                Event::WsHeaders(_) => {}
                event => panic!("unexpected event: {event:?}"),
            }
        };
        assert_eq!(uid, "request-1");
        assert_eq!(form["status"], "200");
        assert_eq!(form["body"], "metric 1\n");
        assert_eq!(headers[CF_ACCESS_CLIENT_ID], "client-id");
        assert_eq!(headers[CF_ACCESS_CLIENT_SECRET], "client-secret");
        cancellation.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn v3_broadcast_requests_get_one_response_each_with_one_exporter_call() {
        let (base, mut events, exporter_stats) =
            start_server_with_requests(3, false, &["request-1", "request-2", "request-3"]).await;
        let cancellation = CancellationToken::new();
        let task = tokio::spawn({
            let context = context(&base, 3, false);
            let cancellation = cancellation.clone();
            async move { run_connection("worker-3", &context, &cancellation).await }
        });

        let _ = take_register(&mut events).await;
        let _ = take_ready(&mut events).await;
        let mut responses = HashMap::new();
        while responses.len() < 3 {
            match recv_event(&mut events).await {
                Event::HttpResponse { uid, form, .. } => {
                    let status = form["status"].parse::<u16>().unwrap();
                    assert!(
                        responses.insert(uid, status).is_none(),
                        "duplicate response"
                    );
                }
                Event::Pong => {}
                Event::WsResponse(response) => {
                    panic!("unexpected WS fallback response: {response}")
                }
                event => panic!("unexpected event: {event:?}"),
            }
        }

        assert_eq!(responses["request-1"], 200);
        assert_eq!(responses["request-2"], 503);
        assert_eq!(responses["request-3"], 503);
        assert_eq!(exporter_stats.calls.load(Ordering::SeqCst), 1);
        assert_eq!(exporter_stats.max_active.load(Ordering::SeqCst), 1);
        assert_eq!(exporter_stats.active.load(Ordering::SeqCst), 0);

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        while let Ok(event) = events.try_recv() {
            assert!(
                !matches!(event, Event::HttpResponse { .. } | Event::WsResponse(_)),
                "duplicate response event: {event:?}"
            );
        }
        cancellation.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn oversized_busy_uid_is_ignored_before_queue_or_post() {
        let oversized_uid = "u".repeat(129);
        let (base, mut events, exporter_stats) =
            start_server_with_requests(3, false, &["request-1", &oversized_uid]).await;
        let cancellation = CancellationToken::new();
        let task = tokio::spawn({
            let context = context(&base, 3, false);
            let cancellation = cancellation.clone();
            async move { run_connection("worker-3", &context, &cancellation).await }
        });

        let _ = take_register(&mut events).await;
        let _ = take_ready(&mut events).await;
        let mut pong_seen = false;
        let response_uid = loop {
            match recv_event(&mut events).await {
                Event::Pong => pong_seen = true,
                Event::HttpResponse { uid, .. } => break uid,
                Event::WsResponse(response) => {
                    panic!("unexpected WS response for oversized UID: {response}")
                }
                event => panic!("unexpected event: {event:?}"),
            }
        };

        assert_eq!(response_uid, "request-1");
        assert!(pong_seen);
        assert_eq!(exporter_stats.calls.load(Ordering::SeqCst), 1);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        while let Ok(event) = events.try_recv() {
            assert!(
                !matches!(event, Event::HttpResponse { .. } | Event::WsResponse(_)),
                "oversized UID produced a response: {event:?}"
            );
        }
        cancellation.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn v3_full_busy_queue_uses_ws_without_losing_uids() {
        let uid_values = (0..16)
            .map(|index| format!("request-{index}"))
            .collect::<Vec<_>>();
        let uid_refs = uid_values.iter().map(String::as_str).collect::<Vec<_>>();
        let (base, mut events, exporter_stats) = start_server_with_requests_and_delay(
            3,
            false,
            &uid_refs,
            std::time::Duration::from_millis(200),
        )
        .await;
        let cancellation = CancellationToken::new();
        let task = tokio::spawn({
            let context = context(&base, 3, false);
            let cancellation = cancellation.clone();
            async move { run_connection("worker-3", &context, &cancellation).await }
        });

        let _ = take_register(&mut events).await;
        let _ = take_ready(&mut events).await;
        let mut responses = HashMap::new();
        let mut ws_fallbacks = 0;
        while responses.len() < uid_values.len() {
            let (uid, status) = match recv_event(&mut events).await {
                Event::HttpResponse { uid, form, .. } => {
                    (uid, form["status"].parse::<u16>().unwrap())
                }
                Event::WsResponse(response) => {
                    ws_fallbacks += 1;
                    (
                        response["uid"].as_str().unwrap().to_owned(),
                        response["status"].as_u64().unwrap() as u16,
                    )
                }
                Event::Pong => continue,
                event => panic!("unexpected event: {event:?}"),
            };
            assert!(
                responses.insert(uid, status).is_none(),
                "duplicate response"
            );
        }

        assert!(ws_fallbacks > 0, "test did not exercise a full queue");
        assert_eq!(responses["request-0"], 200);
        for uid in uid_values.iter().skip(1) {
            assert_eq!(responses[uid], 503);
        }
        assert_eq!(exporter_stats.calls.load(Ordering::SeqCst), 1);
        assert_eq!(exporter_stats.max_active.load(Ordering::SeqCst), 1);
        cancellation.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn v3_failed_http_delivery_falls_back_to_ws_response() {
        let (base, mut events, exporter_stats) =
            start_server_with_requests(3, true, &["request-1", "request-2"]).await;
        let cancellation = CancellationToken::new();
        let task = tokio::spawn({
            let context = context(&base, 3, false);
            let cancellation = cancellation.clone();
            async move { run_connection("worker-3", &context, &cancellation).await }
        });

        let _ = take_register(&mut events).await;
        let _ = take_ready(&mut events).await;
        let mut attempted = HashMap::new();
        let mut fallbacks = HashMap::new();
        while attempted.len() < 2 || fallbacks.len() < 2 {
            match recv_event(&mut events).await {
                Event::HttpResponse { uid, form, .. } => {
                    let status = form["status"].parse::<u16>().unwrap();
                    assert!(attempted.insert(uid, status).is_none(), "duplicate POST");
                }
                Event::WsResponse(response) => {
                    let uid = response["uid"].as_str().unwrap().to_owned();
                    let status = response["status"].as_u64().unwrap() as u16;
                    assert!(
                        fallbacks.insert(uid, status).is_none(),
                        "duplicate fallback"
                    );
                }
                Event::Pong => {}
                event => panic!("unexpected event: {event:?}"),
            }
        }
        assert_eq!(attempted["request-1"], 200);
        assert_eq!(attempted["request-2"], 503);
        assert_eq!(fallbacks, attempted);
        assert_eq!(exporter_stats.calls.load(Ordering::SeqCst), 1);
        assert_eq!(exporter_stats.max_active.load(Ordering::SeqCst), 1);
        cancellation.cancel();
        task.await.unwrap().unwrap();
    }

    struct PendingSink;

    impl futures_util::Sink<tokio_tungstenite::tungstenite::Message> for PendingSink {
        type Error = std::io::Error;

        fn poll_ready(
            self: std::pin::Pin<&mut Self>,
            _context: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Pending
        }

        fn start_send(
            self: std::pin::Pin<&mut Self>,
            _item: tokio_tungstenite::tungstenite::Message,
        ) -> Result<(), Self::Error> {
            Ok(())
        }

        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _context: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn poll_close(
            self: std::pin::Pin<&mut Self>,
            _context: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn websocket_send_is_bounded_by_timeout() {
        let started = tokio::time::Instant::now();
        let result = super::send_ws(
            &mut PendingSink,
            tokio_tungstenite::tungstenite::Message::Text("x".into()),
        )
        .await;
        assert!(result.is_err());
        assert!(started.elapsed() < std::time::Duration::from_secs(1));
    }

    #[test]
    fn oversized_serialized_ws_body_becomes_bounded_500() {
        let raw_response = crate::protocol::ResourceResponse::new(
            "uid".into(),
            200,
            "\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\".into(),
        );
        let budget = MemoryBudget::new(128);
        let mut response = BudgetedResponse::try_new(
            &budget,
            raw_response.uid,
            raw_response.status,
            raw_response.body,
        )
        .unwrap();
        let encoded = super::bounded_response_json_with_limit_and_encoder(
            &mut response,
            &budget,
            64,
            crate::protocol::response_json,
        )
        .unwrap();
        let value: Value = serde_json::from_str(&encoded.json).unwrap();
        assert_eq!(value["status"], 500);
        assert_eq!(value["body"], "");
        assert!(encoded.json.len() <= 64);
    }

    #[test]
    fn ws_precheck_never_serializes_the_oversized_original() {
        let budget = MemoryBudget::new(128);
        let mut response =
            BudgetedResponse::try_new(&budget, "uid".into(), 200, "\u{0001}".repeat(8)).unwrap();
        let encoded_inputs = RefCell::new(Vec::new());

        let encoded = super::bounded_response_json_with_limit_and_encoder(
            &mut response,
            &budget,
            64,
            |candidate| {
                encoded_inputs
                    .borrow_mut()
                    .push((candidate.status, candidate.body.len()));
                crate::protocol::response_json(candidate)
            },
        )
        .unwrap();

        assert_eq!(*encoded_inputs.borrow(), vec![(500, 0)]);
        let value: Value = serde_json::from_str(&encoded.json).unwrap();
        assert_eq!(value["status"], 500);
        assert_eq!(value["body"], "");
        assert_eq!(
            budget.high_watermark(),
            response.retained_bytes() + encoded.reservation.bytes()
        );
        drop(encoded);
        drop(response);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn form_precheck_runs_before_the_eager_encoder() {
        let budget = MemoryBudget::new(50);
        let mut context = context("http://example.test", 3, false);
        context.memory_budget = budget.clone();
        let response =
            BudgetedResponse::try_new(&budget, "uid".into(), 200, "\u{0000}".repeat(8)).unwrap();
        let encoder_calls = Cell::new(0);

        let result =
            super::prepare_form_request_with_encoder(&context, &response, |_response, _length| {
                encoder_calls.set(encoder_calls.get() + 1);
                Ok(String::new())
            });

        assert!(result.is_err());
        assert_eq!(encoder_calls.get(), 0);
        assert_eq!(budget.used(), response.retained_bytes());
        assert_eq!(budget.high_watermark(), response.retained_bytes());
    }

    #[test]
    fn worker_context_clones_share_one_process_budget() {
        let mut first = context("http://example.test", 3, false);
        first.memory_budget = MemoryBudget::new(10);
        let second = first.clone();

        let reservation = first.memory_budget.try_reserve(6).unwrap();
        assert!(second.memory_budget.try_reserve(5).is_err());
        assert_eq!(second.memory_budget.used(), 6);
        drop(reservation);
        assert_eq!(first.memory_budget.used(), 0);
    }

    async fn take_register(events: &mut mpsc::Receiver<Event>) -> Value {
        loop {
            match events.recv().await.unwrap() {
                Event::Register(value) => return value,
                Event::WsHeaders(_) => {}
                event => panic!("unexpected event: {event:?}"),
            }
        }
    }

    async fn take_ready(events: &mut mpsc::Receiver<Event>) -> Value {
        match events.recv().await.unwrap() {
            Event::Ready(value) => value,
            event => panic!("unexpected event: {event:?}"),
        }
    }

    async fn recv_event(events: &mut mpsc::Receiver<Event>) -> Event {
        tokio::time::timeout(std::time::Duration::from_secs(2), events.recv())
            .await
            .expect("timed out waiting for test event")
            .expect("test event channel closed")
    }
}
