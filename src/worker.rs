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
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, Message, WebSocketConfig};
use tokio_util::sync::CancellationToken;

const CF_ACCESS_CLIENT_ID: &str = "CF-Access-Client-Id";
const CF_ACCESS_CLIENT_SECRET: &str = "CF-Access-Client-Secret";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const RECONNECT_DELAY: Duration = Duration::from_secs(1);
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(30);
/// A connection that lived at least this long resets the reconnect backoff.
const STABLE_CONNECTION: Duration = Duration::from_secs(60);
const CONTROL_QUEUE_CAPACITY: usize = 32;
const BULK_QUEUE_CAPACITY: usize = 8;
const BUSY_RESPONSE_QUEUE_CAPACITY: usize = 8;
const BUSY_RESPONSE_CONCURRENCY: usize = 4;
/// Response bodies get extra send time as if the link carried at least this
/// many bytes per second.
const MIN_BULK_THROUGHPUT: f64 = 64.0 * 1024.0;
/// Matches the server heartbeat timeout: the server hears nothing else on
/// this connection while one large frame is uploading.
const MAX_BULK_SEND_TIMEOUT: Duration = Duration::from_secs(45);
const MAX_POST_TIMEOUT: Duration = Duration::from_secs(60);
/// After a POST timed out the path is slow; resending a larger body over the
/// WebSocket doubles the upload and arrives after the scrape has ended.
const FALLBACK_AFTER_TIMEOUT_LIMIT: usize = 256 * 1024;

/// Connection timing. Defaults match the server: it pings every 15 seconds
/// and drops a connection after 45 silent seconds.
#[derive(Clone, Copy, Debug)]
pub struct Timing {
    pub heartbeat_interval: Duration,
    /// The connection is considered dead after this long without any frame
    /// from the server.
    pub heartbeat_timeout: Duration,
    /// Send timeout for small messages; bulk responses scale it by size.
    pub send_timeout: Duration,
    /// Base time for one wire-v3 response POST, scaled by body size like
    /// WebSocket responses. A POST stuck on a dead pooled connection no
    /// longer keeps the worker busy for the 60 second exporter timeout.
    pub post_timeout: Duration,
    /// Time allowed to flush a close frame before dropping the connection.
    pub close_timeout: Duration,
}

impl Default for Timing {
    fn default() -> Self {
        Self {
            heartbeat_interval: Duration::from_secs(20),
            heartbeat_timeout: Duration::from_secs(45),
            send_timeout: Duration::from_secs(5),
            post_timeout: Duration::from_secs(10),
            close_timeout: Duration::from_secs(1),
        }
    }
}

#[derive(Clone)]
pub struct WorkerContext {
    pub config: Arc<Config>,
    pub target: Target,
    pub client: Arc<reqwest::Client>,
    pub protocol: u8,
    pub memory_budget: MemoryBudget,
    pub timing: Timing,
}

pub async fn run_worker(worker_name: String, context: WorkerContext, shutdown: CancellationToken) {
    tracing::info!(worker = %worker_name, "worker started");
    let mut quick_failures = 0;
    while !shutdown.is_cancelled() {
        let started = Instant::now();
        if let Err(error) = run_connection(&worker_name, &context, &shutdown).await {
            if !shutdown.is_cancelled() {
                tracing::warn!(worker = %worker_name, error = %error, "connection ended");
            }
        }
        if started.elapsed() >= STABLE_CONNECTION {
            quick_failures = 0;
        } else {
            quick_failures += 1;
        }
        let delay = reconnect_delay(quick_failures, random_fraction());
        tokio::select! {
            () = shutdown.cancelled() => break,
            () = tokio::time::sleep(delay) => {}
        }
    }
    tracing::info!(worker = %worker_name, "worker stopped");
}

/// Exponential backoff for connections that keep failing quickly, with
/// jitter so that a network-wide disconnect does not reconnect every worker
/// in the same instant. `fraction` is uniformly distributed in `[0, 1)`.
fn reconnect_delay(quick_failures: u32, fraction: f64) -> Duration {
    let exponent = quick_failures.saturating_sub(1).min(5);
    let base = RECONNECT_DELAY
        .saturating_mul(1 << exponent)
        .min(MAX_RECONNECT_DELAY);
    base.mul_f64(0.5 + fraction.clamp(0.0, 1.0) / 2.0)
}

/// Uniform in `[0, 1)` from the 48 leading random bits of a v4 UUID; the
/// version and variant bits come later.
fn random_fraction() -> f64 {
    (uuid::Uuid::new_v4().as_u128() >> 80) as f64 / (1u64 << 48) as f64
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
        connect_async_with_config(request, Some(websocket_config), true),
    )
    .await
    .map_err(|_| "websocket connect timed out")??;
    let (write, read) = websocket.split();

    let (control_tx, control_rx) = mpsc::channel::<Message>(CONTROL_QUEUE_CAPACITY);
    let (bulk_tx, bulk_rx) = mpsc::channel::<EncodedResponse>(BULK_QUEUE_CAPACITY);
    control_tx
        .try_send(Message::Text(
            register_json(context.protocol, &context.config.instance, worker_name)?.into(),
        ))
        .map_err(|_| "register message could not be queued")?;
    let writer = tokio::spawn(run_writer(
        write,
        control_rx,
        bulk_rx,
        context.timing.send_timeout,
    ));
    tracing::info!(worker = %worker_name, "connected and registered");

    let (busy_response_tx, busy_response_rx) =
        mpsc::channel::<BudgetedResponse>(BUSY_RESPONSE_QUEUE_CAPACITY);
    let busy_response_actor = tokio::spawn(run_busy_response_actor(
        context.clone(),
        busy_response_rx,
        bulk_tx.clone(),
    ));
    let mut session = Session {
        worker_name,
        context,
        control: control_tx,
        bulk: bulk_tx,
        busy_responses: busy_response_tx,
        writer,
        writer_done: false,
        active_task: None,
    };
    let (result, farewell) = session.run(read, shutdown).await;
    session.finish(farewell).await;
    busy_response_actor.abort();
    let _ = busy_response_actor.await;
    result
}

/// State of one connection. Reading happens here; every write goes through
/// the writer task, so a slow upload never delays reading server frames.
struct Session<'a> {
    worker_name: &'a str,
    context: &'a WorkerContext,
    control: mpsc::Sender<Message>,
    bulk: mpsc::Sender<EncodedResponse>,
    busy_responses: mpsc::Sender<BudgetedResponse>,
    writer: JoinHandle<Result<(), BoxError>>,
    writer_done: bool,
    /// The scrape in progress: exporter call and, for wire v3, the response
    /// POST. Staying busy until the POST ends keeps uploads from piling up
    /// on a slow link.
    active_task: Option<JoinHandle<()>>,
}

type Farewell = Option<Message>;

fn close_message(code: CloseCode, reason: &'static str) -> Message {
    Message::Close(Some(CloseFrame {
        code,
        reason: reason.into(),
    }))
}

impl Session<'_> {
    async fn run<R>(
        &mut self,
        mut read: R,
        shutdown: &CancellationToken,
    ) -> (Result<(), BoxError>, Farewell)
    where
        R: futures_util::Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>>
            + Unpin,
    {
        let timing = self.context.timing;
        let (completion_tx, mut completion_rx) = mpsc::channel::<Option<BudgetedResponse>>(1);
        let mut heartbeat = tokio::time::interval(timing.heartbeat_interval);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        heartbeat.tick().await;
        let mut last_inbound = Instant::now();

        loop {
            tokio::select! {
                () = shutdown.cancelled() => {
                    return (Ok(()), Some(close_message(CloseCode::Away, "client shutdown")));
                }
                written = &mut self.writer => {
                    self.writer_done = true;
                    let error = match written {
                        Ok(Ok(())) => "websocket writer stopped".into(),
                        Ok(Err(error)) => error,
                        Err(error) => Box::new(error) as BoxError,
                    };
                    return (Err(error), None);
                }
                _ = heartbeat.tick() => {
                    if last_inbound.elapsed() > timing.heartbeat_timeout {
                        return (
                            Err("websocket heartbeat timed out".into()),
                            Some(close_message(CloseCode::Away, "heartbeat timeout")),
                        );
                    }
                    let _ = self.control.try_send(Message::Text(PING_JSON.into()));
                    let _ = self
                        .control
                        .try_send(Message::Ping(self.worker_name.as_bytes().to_vec().into()));
                }
                completion = completion_rx.recv(), if self.active_task.is_some() => {
                    let Some(response) = completion else {
                        return (Err("resource task channel closed".into()), None);
                    };
                    if let Some(task) = self.active_task.take() {
                        if let Err(error) = task.await {
                            return (Err(error.into()), None);
                        }
                    }
                    if let Some(response) = response {
                        if let Err(error) =
                            queue_ws_response(&self.bulk, &self.context.memory_budget, response).await
                        {
                            return (Err(error), None);
                        }
                    }
                }
                message = read.next() => {
                    let message = match message {
                        Some(Ok(message)) => message,
                        Some(Err(error)) => return (Err(error.into()), None),
                        None => return (Err("websocket closed without a close frame".into()), None),
                    };
                    last_inbound = Instant::now();
                    match message {
                        Message::Text(text) => {
                            if let Err(error) = self.handle_text(text.as_str(), &completion_tx).await {
                                return (Err(error), None);
                            }
                        }
                        Message::Ping(payload) => {
                            let _ = self.control.try_send(Message::Pong(payload));
                        }
                        Message::Pong(_) => {}
                        Message::Close(frame) => {
                            tracing::warn!(
                                worker = %self.worker_name,
                                code = frame.as_ref().map(|frame| u16::from(frame.code)),
                                reason = frame.as_ref().map(|frame| frame.reason.as_str()),
                                "server closed the connection"
                            );
                            return (Ok(()), Some(Message::Close(frame)));
                        }
                        Message::Binary(_) | Message::Frame(_) => {
                            tracing::warn!("ignored unsupported websocket message");
                        }
                    }
                }
            }
        }
    }

    async fn handle_text(
        &mut self,
        text: &str,
        completion_tx: &mpsc::Sender<Option<BudgetedResponse>>,
    ) -> Result<(), BoxError> {
        let incoming = match serde_json::from_str::<IncomingMessage>(text) {
            Ok(incoming) => incoming,
            Err(error) => {
                tracing::warn!(error = %error, "ignored invalid websocket JSON");
                return Ok(());
            }
        };
        if let Err(error) = incoming.validate() {
            tracing::warn!(error = %error, "ignored websocket message with oversized identifier");
            return Ok(());
        }
        match incoming {
            IncomingMessage::Ping => {
                let _ = self.control.try_send(Message::Text(PONG_JSON.into()));
            }
            IncomingMessage::Pong => {}
            IncomingMessage::Ready { uid } => {
                if self.context.protocol >= 2 && self.active_task.is_none() {
                    let ready = ready_json(&uid, self.worker_name)?;
                    if self.control.try_send(Message::Text(ready.into())).is_err() {
                        tracing::warn!("ready answer dropped because the send queue is full");
                    }
                } else {
                    tracing::warn!("ignored ready message while worker is busy");
                }
            }
            IncomingMessage::Request { uid, resource } => {
                if self.active_task.is_some() {
                    tracing::warn!("rejected a second request while worker is busy");
                    match BudgetedResponse::try_new(
                        &self.context.memory_budget,
                        uid,
                        503,
                        String::new(),
                    ) {
                        Ok(busy) => self.answer_busy(busy).await?,
                        Err(error) => {
                            tracing::warn!(error = %error, "busy response rejected by memory budget");
                        }
                    }
                    return Ok(());
                }

                let task_context = self.context.clone();
                let task_completion_tx = completion_tx.clone();
                self.active_task = Some(tokio::spawn(async move {
                    let response = call_resource(
                        &task_context.client,
                        &task_context.config,
                        uid,
                        &resource,
                        &task_context.memory_budget,
                    )
                    .await;
                    let websocket_response = match response {
                        Some(response) if task_context.protocol == 3 => {
                            deliver_by_post(&task_context, response).await
                        }
                        response => response,
                    };
                    let _ = task_completion_tx.send(websocket_response).await;
                }));
            }
            IncomingMessage::Unknown => tracing::warn!("ignored unknown websocket message"),
        }
        Ok(())
    }

    /// Wire v3 posts busy answers in the background; older wires, and posts
    /// that cannot be queued, answer over the WebSocket.
    async fn answer_busy(&self, response: BudgetedResponse) -> Result<(), BoxError> {
        let mut response = response;
        if self.context.protocol == 3 {
            match self.busy_responses.try_send(response) {
                Ok(()) => return Ok(()),
                Err(error) => response = error.into_inner(),
            }
        }
        queue_ws_response(&self.bulk, &self.context.memory_budget, response).await
    }

    /// Sends the farewell frame if the writer is still healthy, then stops it.
    async fn finish(mut self, farewell: Farewell) {
        if let Some(task) = self.active_task.take() {
            task.abort();
            let _ = task.await;
        }
        if self.writer_done {
            return;
        }
        drop(self.bulk);
        drop(self.busy_responses);
        let queued = farewell.is_some_and(|message| self.control.try_send(message).is_ok());
        drop(self.control);
        let flushed = queued
            && tokio::time::timeout(self.context.timing.close_timeout, &mut self.writer)
                .await
                .is_ok();
        if !flushed {
            self.writer.abort();
            let _ = self.writer.await;
        }
    }
}

/// Queues a WebSocket response. A response that cannot be encoded within the
/// memory budget is dropped with a warning instead of ending the connection.
async fn queue_ws_response(
    bulk: &mpsc::Sender<EncodedResponse>,
    memory_budget: &MemoryBudget,
    mut response: BudgetedResponse,
) -> Result<(), BoxError> {
    let encoded = match bounded_response_json(&mut response, memory_budget) {
        Ok(encoded) => encoded,
        Err(error) => {
            tracing::warn!(uid = response.uid, error = %error, "websocket response dropped");
            return Ok(());
        }
    };
    drop(response);
    bulk.send(encoded)
        .await
        .map_err(|_| "websocket writer stopped".into())
}

/// Owns the write half. Control frames (heartbeats, ready answers, close)
/// go before queued response bodies.
async fn run_writer<S>(
    mut sink: S,
    mut control: mpsc::Receiver<Message>,
    mut bulk: mpsc::Receiver<EncodedResponse>,
    send_timeout: Duration,
) -> Result<(), BoxError>
where
    S: Sink<Message> + Unpin,
    S::Error: std::error::Error + Send + Sync + 'static,
{
    let mut control_open = true;
    let mut bulk_open = true;
    loop {
        tokio::select! {
            biased;
            message = control.recv(), if control_open => match message {
                Some(message @ Message::Close(_)) => {
                    // Sending fails when the server closed first; closing the
                    // sink still flushes the reply tungstenite queued for it.
                    let _ = send_ws(&mut sink, message, send_timeout).await;
                    let _ = tokio::time::timeout(send_timeout, sink.close()).await;
                    return Ok(());
                }
                Some(message) => send_ws(&mut sink, message, send_timeout).await?,
                None => control_open = false,
            },
            encoded = bulk.recv(), if bulk_open => match encoded {
                Some(EncodedResponse { json, reservation }) => {
                    let timeout = bulk_send_timeout(send_timeout, json.len());
                    let result = send_ws(&mut sink, Message::Text(json.into()), timeout).await;
                    drop(reservation);
                    result?;
                }
                None => bulk_open = false,
            },
            else => return Ok(()),
        }
    }
}

/// A response body needs time proportional to its size on a slow link; a
/// fixed timeout would drop healthy high-latency connections.
fn bulk_send_timeout(send_timeout: Duration, length: usize) -> Duration {
    scaled_timeout(send_timeout, length, MAX_BULK_SEND_TIMEOUT)
}

fn post_timeout(base: Duration, length: usize) -> Duration {
    scaled_timeout(base, length, MAX_POST_TIMEOUT)
}

fn scaled_timeout(base: Duration, length: usize, cap: Duration) -> Duration {
    let transfer = Duration::from_secs_f64(length as f64 / MIN_BULK_THROUGHPUT);
    base.saturating_add(transfer).min(cap).max(base)
}

/// Posts a wire-v3 response and returns it when it should be sent over the
/// WebSocket instead.
async fn deliver_by_post(
    context: &WorkerContext,
    response: BudgetedResponse,
) -> Option<BudgetedResponse> {
    let error = match post_response(context, &response).await {
        Ok(()) => return None,
        Err(error) => error,
    };
    let timed_out = error
        .downcast_ref::<reqwest::Error>()
        .is_some_and(reqwest::Error::is_timeout);
    if timed_out && response.body.len() > FALLBACK_AFTER_TIMEOUT_LIMIT {
        tracing::warn!(
            uid = response.uid,
            body_bytes = response.body.len(),
            error = %error,
            "v3 response POST timed out; not resending the large body over websocket"
        );
        return None;
    }
    tracing::warn!(
        uid = response.uid,
        error = %error,
        "unable to deliver v3 response; falling back to websocket"
    );
    Some(response)
}

async fn run_busy_response_actor(
    context: WorkerContext,
    mut responses: mpsc::Receiver<BudgetedResponse>,
    bulk: mpsc::Sender<EncodedResponse>,
) {
    let mut posts = FuturesUnordered::new();
    let mut input_open = true;

    loop {
        if !input_open && posts.is_empty() {
            return;
        }

        tokio::select! {
            response = responses.recv(), if input_open && posts.len() < BUSY_RESPONSE_CONCURRENCY => {
                match response {
                    Some(response) => {
                        let post_context = context.clone();
                        posts.push(async move {
                            let result = post_response(&post_context, &response).await;
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
                    if queue_ws_response(&bulk, &context.memory_budget, response)
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
        }
    }
}

async fn send_ws<S>(sink: &mut S, message: Message, timeout: Duration) -> Result<(), BoxError>
where
    S: Sink<Message> + Unpin,
    S::Error: std::error::Error + Send + Sync + 'static,
{
    tokio::time::timeout(timeout, sink.send(message))
        .await
        .map_err(|_| "websocket send timed out")?
        .map_err(|error| Box::new(error) as BoxError)
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
        .timeout(post_timeout(context.timing.post_timeout, encoded_length))
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
    use super::{
        run_connection, Timing, WorkerContext, CF_ACCESS_CLIENT_ID, CF_ACCESS_CLIENT_SECRET,
    };
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
            timing: Timing {
                send_timeout: std::time::Duration::from_millis(100),
                ..Timing::default()
            },
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
            std::time::Duration::from_millis(100),
        )
        .await;
        assert_eq!(result.unwrap_err().to_string(), "websocket send timed out");
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

    /// One accepted WebSocket connection, driven from the test body.
    struct ScriptedSocket {
        to_client: mpsc::Sender<Message>,
        from_client: mpsc::Receiver<Message>,
    }

    #[derive(Clone)]
    struct ScriptedState {
        sockets: mpsc::Sender<ScriptedSocket>,
        posts: mpsc::Sender<(String, HashMap<String, String>)>,
        post_delay: std::time::Duration,
    }

    struct Scripted {
        base: String,
        sockets: mpsc::Receiver<ScriptedSocket>,
        posts: mpsc::Receiver<(String, HashMap<String, String>)>,
    }

    async fn scripted_ws(
        State(state): State<ScriptedState>,
        upgrade: WebSocketUpgrade,
    ) -> Response {
        upgrade.on_upgrade(move |socket| async move {
            let (mut sink, mut stream) = socket.split();
            let (to_client, mut outgoing) = mpsc::channel::<Message>(32);
            let (incoming, from_client) = mpsc::channel::<Message>(64);
            let _ = state
                .sockets
                .send(ScriptedSocket {
                    to_client,
                    from_client,
                })
                .await;
            let writer = tokio::spawn(async move {
                use futures_util::SinkExt;
                while let Some(message) = outgoing.recv().await {
                    if sink.send(message).await.is_err() {
                        break;
                    }
                }
            });
            while let Some(Ok(message)) = stream.next().await {
                if incoming.send(message).await.is_err() {
                    break;
                }
            }
            writer.abort();
        })
    }

    async fn scripted_post(
        State(state): State<ScriptedState>,
        Path(uid): Path<String>,
        Form(form): Form<HashMap<String, String>>,
    ) -> axum::http::StatusCode {
        tokio::time::sleep(state.post_delay).await;
        let _ = state.posts.send((uid, form)).await;
        axum::http::StatusCode::OK
    }

    async fn big_exporter() -> String {
        "x".repeat(300 * 1024)
    }

    async fn quick_exporter() -> &'static str {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        "metric 2\n"
    }

    async fn start_scripted(post_delay: std::time::Duration) -> Scripted {
        let (sockets_tx, sockets) = mpsc::channel(4);
        let (posts_tx, posts) = mpsc::channel(16);
        let state = ScriptedState {
            sockets: sockets_tx,
            posts: posts_tx,
            post_delay,
        };
        let app = Router::new()
            .route("/proxy/ws/", any(scripted_ws))
            .route("/proxy/response/{uid}/", post(scripted_post))
            .route("/metrics", get(quick_exporter))
            .route("/big", get(big_exporter))
            .with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Scripted {
            base: format!("http://{address}"),
            sockets,
            posts,
        }
    }

    impl ScriptedSocket {
        async fn send_json(&self, value: Value) {
            self.to_client
                .send(Message::Text(value.to_string().into()))
                .await
                .unwrap();
        }

        /// Next JSON message that is not a heartbeat.
        async fn next_json(&mut self) -> Value {
            loop {
                let message = tokio::time::timeout(
                    std::time::Duration::from_secs(2),
                    self.from_client.recv(),
                )
                .await
                .expect("timed out waiting for a client message")
                .expect("client connection ended");
                if let Message::Text(text) = message {
                    let value: Value = serde_json::from_str(text.as_str()).unwrap();
                    if !matches!(value["type"].as_str(), Some("ping" | "pong")) {
                        return value;
                    }
                }
            }
        }
    }

    fn scripted_context(base: &str, protocol: u8, timing: Timing) -> WorkerContext {
        let mut context = context(base, protocol, false);
        context.timing = timing;
        context
    }

    #[tokio::test]
    async fn slow_v3_post_is_bounded_and_falls_back_to_websocket() {
        let mut server = start_scripted(std::time::Duration::from_secs(5)).await;
        let cancellation = CancellationToken::new();
        let timing = Timing {
            post_timeout: std::time::Duration::from_millis(300),
            ..Timing::default()
        };
        let task = tokio::spawn({
            let context = scripted_context(&server.base, 3, timing);
            let cancellation = cancellation.clone();
            async move { run_connection("worker-3", &context, &cancellation).await }
        });
        let mut socket = server.sockets.recv().await.unwrap();
        assert_eq!(socket.next_json().await["type"], "register");
        socket
            .send_json(json!({"type":"request","uid":"q1","resource":"node"}))
            .await;
        let started = tokio::time::Instant::now();
        let response = socket.next_json().await;
        assert_eq!(response["type"], "response");
        assert_eq!(response["uid"], "q1");
        assert_eq!(response["body"], "metric 2\n");
        assert!(started.elapsed() < std::time::Duration::from_millis(1500));
        cancellation.cancel();
        task.await.unwrap().unwrap();
        assert!(server.posts.try_recv().is_err());
    }

    #[tokio::test]
    async fn timed_out_large_post_is_not_resent_over_websocket() {
        let mut server = start_scripted(std::time::Duration::from_secs(30)).await;
        let cancellation = CancellationToken::new();
        let timing = Timing {
            post_timeout: std::time::Duration::from_millis(100),
            ..Timing::default()
        };
        let mut context = scripted_context(&server.base, 3, timing);
        let mut config = (*context.config).clone();
        config
            .resources
            .insert("big".into(), format!("{}/big", server.base));
        context.config = Arc::new(config);
        let task = tokio::spawn({
            let cancellation = cancellation.clone();
            async move { run_connection("worker-3", &context, &cancellation).await }
        });
        let mut socket = server.sockets.recv().await.unwrap();
        assert_eq!(socket.next_json().await["type"], "register");
        socket
            .send_json(json!({"type":"request","uid":"q1","resource":"big"}))
            .await;
        // 300 KiB gets about 4.8 seconds before the POST times out.
        tokio::time::sleep(std::time::Duration::from_millis(5500)).await;
        socket.send_json(json!({"type":"ready","uid":"r2"})).await;
        let next = socket.next_json().await;
        assert_eq!(next, json!({"type":"ready","uid":"r2","worker":"worker-3"}));
        assert!(server.posts.try_recv().is_err());
        cancellation.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn server_close_frame_is_answered_and_is_not_an_error() {
        let mut server = start_scripted(std::time::Duration::ZERO).await;
        let cancellation = CancellationToken::new();
        let task = tokio::spawn({
            let context = context(&server.base, 3, false);
            let cancellation = cancellation.clone();
            async move { run_connection("worker-3", &context, &cancellation).await }
        });
        let mut socket = server.sockets.recv().await.unwrap();
        assert_eq!(socket.next_json().await["type"], "register");
        socket
            .to_client
            .send(Message::Close(Some(axum::extract::ws::CloseFrame {
                code: 1001,
                reason: "worker replaced".into(),
            })))
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), task)
            .await
            .expect("client did not end the connection")
            .unwrap()
            .unwrap();
        let reply = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                match socket.from_client.recv().await {
                    Some(Message::Close(frame)) => return frame,
                    Some(_) => continue,
                    None => panic!("no close reply"),
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(reply.map(|frame| frame.code), Some(1001));
    }

    async fn silent_ws(
        State(frames): State<mpsc::Sender<Message>>,
        upgrade: WebSocketUpgrade,
    ) -> Response {
        upgrade.on_upgrade(move |mut socket| async move {
            let _register = socket.next().await;
            // Neither reading nor writing leaves the client without any
            // inbound frame, including automatic pongs.
            tokio::time::sleep(std::time::Duration::from_millis(700)).await;
            while let Some(Ok(message)) = socket.next().await {
                let closing = matches!(message, Message::Close(_));
                let _ = frames.send(message).await;
                if closing {
                    break;
                }
            }
        })
    }

    #[tokio::test]
    async fn heartbeat_timeout_sends_a_close_frame() {
        let (frames_tx, mut frames) = mpsc::channel(64);
        let app = Router::new()
            .route("/proxy/ws/", any(silent_ws))
            .with_state(frames_tx);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        // The first heartbeat tick already sees the timeout, so no ping
        // precedes the close frame; the server's automatic pong to a ping
        // could hit the closed socket and make the close frame unreadable.
        let timing = Timing {
            heartbeat_interval: std::time::Duration::from_millis(100),
            heartbeat_timeout: std::time::Duration::from_millis(10),
            ..Timing::default()
        };
        let context = scripted_context(&format!("http://{address}"), 3, timing);
        let result = run_connection("worker-3", &context, &CancellationToken::new()).await;
        assert!(result.unwrap_err().to_string().contains("heartbeat"));

        let close = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                match frames.recv().await {
                    Some(Message::Close(frame)) => return frame,
                    Some(_) => continue,
                    None => panic!("server saw no close frame"),
                }
            }
        })
        .await
        .unwrap()
        .expect("close frame without a reason");
        assert_eq!(close.code, 1001);
        assert_eq!(close.reason.as_str(), "heartbeat timeout");
    }

    #[derive(Default)]
    struct RecordingSink(Vec<tokio_tungstenite::tungstenite::Message>);

    impl futures_util::Sink<tokio_tungstenite::tungstenite::Message> for RecordingSink {
        type Error = std::io::Error;

        fn poll_ready(
            self: std::pin::Pin<&mut Self>,
            _context: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn start_send(
            self: std::pin::Pin<&mut Self>,
            item: tokio_tungstenite::tungstenite::Message,
        ) -> Result<(), Self::Error> {
            self.get_mut().0.push(item);
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

    fn encoded(budget: &MemoryBudget, uid: &str, body_len: usize) -> super::EncodedResponse {
        let mut response =
            BudgetedResponse::try_new(budget, uid.into(), 200, "x".repeat(body_len)).unwrap();
        super::bounded_response_json(&mut response, budget).unwrap()
    }

    #[tokio::test]
    async fn writer_sends_control_frames_before_queued_response_bodies() {
        use tokio_tungstenite::tungstenite::Message as WsMessage;
        let budget = MemoryBudget::new(1024 * 1024);
        let (control_tx, control_rx) = mpsc::channel(4);
        let (bulk_tx, bulk_rx) = mpsc::channel(4);
        bulk_tx.send(encoded(&budget, "b1", 10)).await.unwrap();
        bulk_tx.send(encoded(&budget, "b2", 10)).await.unwrap();
        control_tx
            .send(WsMessage::Text("control".into()))
            .await
            .unwrap();
        drop((control_tx, bulk_tx));

        let mut sink = RecordingSink::default();
        super::run_writer(
            &mut sink,
            control_rx,
            bulk_rx,
            std::time::Duration::from_secs(1),
        )
        .await
        .unwrap();
        let texts: Vec<_> = sink
            .0
            .iter()
            .map(|message| message.to_text().unwrap().to_owned())
            .collect();
        assert_eq!(texts[0], "control");
        assert!(texts[1].contains("\"b1\"") && texts[2].contains("\"b2\""));
        assert_eq!(budget.used(), 0);
    }

    /// Accepts nothing until `open_at`, like a socket whose send buffer is full.
    struct SlowSink {
        open_at: std::pin::Pin<Box<tokio::time::Sleep>>,
    }

    impl SlowSink {
        fn new(delay: std::time::Duration) -> Self {
            Self {
                open_at: Box::pin(tokio::time::sleep(delay)),
            }
        }
    }

    impl futures_util::Sink<tokio_tungstenite::tungstenite::Message> for SlowSink {
        type Error = std::io::Error;

        fn poll_ready(
            self: std::pin::Pin<&mut Self>,
            context: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::future::Future::poll(self.get_mut().open_at.as_mut(), context).map(Ok)
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
    async fn response_bodies_get_a_size_scaled_send_timeout() {
        let budget = MemoryBudget::new(1024 * 1024);
        let send_timeout = std::time::Duration::from_millis(100);
        let stall = std::time::Duration::from_millis(400);

        let (control_tx, control_rx) = mpsc::channel(1);
        let (_bulk_tx, bulk_rx) = mpsc::channel::<super::EncodedResponse>(1);
        control_tx
            .send(tokio_tungstenite::tungstenite::Message::Text("ping".into()))
            .await
            .unwrap();
        let control_result =
            super::run_writer(SlowSink::new(stall), control_rx, bulk_rx, send_timeout).await;
        assert!(
            control_result.is_err(),
            "small frames keep the short timeout"
        );

        let (control_tx, control_rx) = mpsc::channel(1);
        let (bulk_tx, bulk_rx) = mpsc::channel(1);
        bulk_tx
            .send(encoded(&budget, "big", 64 * 1024))
            .await
            .unwrap();
        drop((control_tx, bulk_tx));
        super::run_writer(SlowSink::new(stall), control_rx, bulk_rx, send_timeout)
            .await
            .expect("a 64 KiB body gets about one extra second");
    }

    #[test]
    fn bulk_send_timeout_scales_with_size_and_is_capped() {
        let base = std::time::Duration::from_secs(5);
        assert_eq!(super::bulk_send_timeout(base, 0), base);
        assert_eq!(
            super::bulk_send_timeout(base, 640 * 1024),
            std::time::Duration::from_secs(15)
        );
        assert_eq!(
            super::bulk_send_timeout(base, 64 * 1024 * 1024),
            std::time::Duration::from_secs(45)
        );
        assert_eq!(
            super::post_timeout(std::time::Duration::from_secs(10), 640 * 1024),
            std::time::Duration::from_secs(20)
        );
        assert_eq!(
            super::post_timeout(std::time::Duration::from_secs(10), 64 * 1024 * 1024),
            std::time::Duration::from_secs(60)
        );
    }

    #[test]
    fn reconnect_delay_backs_off_with_jitter_and_a_cap() {
        let seconds = |failures, fraction| super::reconnect_delay(failures, fraction).as_secs_f64();
        assert_eq!(seconds(0, 0.0), 0.5);
        assert_eq!(seconds(1, 0.0), 0.5);
        assert!(seconds(1, 0.999) < 1.0);
        assert_eq!(seconds(2, 0.0), 1.0);
        assert_eq!(seconds(3, 1.0), 4.0);
        assert_eq!(seconds(6, 1.0), 30.0);
        assert_eq!(seconds(60, 0.0), 15.0);
        for _ in 0..1000 {
            let fraction = super::random_fraction();
            assert!((0.0..1.0).contains(&fraction), "{fraction}");
        }
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
