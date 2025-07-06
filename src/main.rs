use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::Json,
    routing::{get, post, delete},
    Router,
};
use redis::{aio::ConnectionManager, AsyncCommands, Client as RedisClient};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::{
    sync::{Mutex, oneshot},
    time::timeout,
};
use futures_util::stream::StreamExt;

type AppState = Arc<SharedState>;

#[derive(Clone)]
struct SharedState {
    redis: Arc<Mutex<ConnectionManager>>,
    pending: Arc<Mutex<HashMap<String, oneshot::Sender<String>>>>,
}

#[derive(Debug, Deserialize)]
struct PollRequest {
    client_id: String,
    timeout: Option<u64>,
}

#[derive(Debug, Serialize)]
struct PollResponse {
    status: String,
    data: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PushRequest {
    client_id: String,
    message: String,
}

#[tokio::main]
async fn main() {
    // Set up Redis connection
    let redis_url = std::env::var("REDIS_URL").unwrap_or("redis://127.0.0.1/".into());
    let redis_client = RedisClient::open(redis_url).expect("Failed to create Redis client");
    let redis_conn = ConnectionManager::new(redis_client)
        .await
        .expect("Failed to create Redis connection manager");

    // Create shared application state
    let state = Arc::new(SharedState {
        redis: Arc::new(Mutex::new(redis_conn)),
        pending: Arc::new(Mutex::new(HashMap::new())),
    });

    // Start Redis subscription listener
    tokio::spawn(redis_subscriber(state.clone()));

    // Set up HTTP routes
    let app = Router::new()
        .route("/poll", get(handle_poll))
        .route("/push", post(handle_push))
        .route("/disconnect", delete(handle_disconnect))
        .with_state(state);

    // Start HTTP server
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .unwrap();
    println!("listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();
}

// Long polling endpoint handler
async fn handle_poll(
    State(state): State<AppState>,
    Query(params): Query<PollRequest>,
) -> Result<Json<PollResponse>, StatusCode> {
    let client_id = params.client_id;
    let poll_timeout = params.timeout.unwrap_or(45_000); // Default 45 seconds

    // Check Redis for existing messages
    let message: Option<String> = state.redis.lock().await.lpop(format!("messages:{client_id}"), None)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    if let Some(msg) = message {
        return Ok(Json(PollResponse {
            status: "success".into(),
            data: Some(msg),
        }));
    }

    // Set up response channel
    let (tx, rx) = oneshot::channel();
    state.pending.lock().await.insert(client_id.clone(), tx);

    // Wait for message or timeout
    match timeout(Duration::from_millis(poll_timeout), rx).await {
        Ok(Ok(msg)) => Ok(Json(PollResponse {
            status: "success".into(),
            data: Some(msg),
        })),
        Ok(Err(_)) | Err(_) => {
            // Clean up if still in map
            state.pending.lock().await.remove(&client_id);
            Ok(Json(PollResponse {
                status: "timeout".into(),
                data: None,
            }))
        }
    }
}

// Message push endpoint handler
async fn handle_push(
    State(state): State<AppState>,
    axum::Json(payload): axum::Json<PushRequest>,
) -> StatusCode {
    let client_id = payload.client_id;
    let message = payload.message;

    // Check for pending request
    if let Some(tx) = state.pending.lock().await.remove(&client_id) {
        let _ = tx.send(message.clone()); // Ignore errors
        return StatusCode::OK;
    }

    // Store message in Redis if no active connection
    let _: Result<String, ()> = state.redis.lock().await
        .rpush(format!("messages:{client_id}"), &message)
        .await
        .map_err(|_| ());
    
    // Set TTL for message storage
    let _ : Result<String, ()> = state.redis.lock().await
        .expire(format!("messages:{client_id}"), 3600)
        .await
        .map_err(|_| ());
    
    StatusCode::OK
}

// Client disconnect handler
async fn handle_disconnect(
    State(state): State<AppState>,
    Query(params): Query<PollRequest>,
) -> StatusCode {
    let client_id = params.client_id;
    
    // Remove from pending requests
    state.pending.lock().await.remove(&client_id);
    
    // Remove from Redis
    let _ : Result<String, ()> = state.redis.lock().await
        .del(format!("messages:{client_id}"))
        .await
        .map_err(|_| ());
    
    StatusCode::OK
}

// Redis subscription listener
async fn redis_subscriber(state: AppState) {
    let client = RedisClient::open(
        std::env::var("REDIS_URL").unwrap_or("redis://127.0.0.1/".into())
    ).expect("Failed to create Redis client");
    
    let mut pubsub = client.get_async_pubsub().await
        .expect("Failed to connect to Redis");
    
    pubsub.subscribe("longpolling").await
        .expect("Failed to subscribe to channel");

    let mut stram = pubsub.on_message();

    while let Some(msg) = stram.next().await {
        let payload: String = match msg.get_payload() {
            Ok(p) => p,
            Err(_) => continue,
        };

        if let Ok((client_id, message)) = serde_json::from_str::<(String, String)>(&payload) {
            if let Some(tx) = state.pending.lock().await.remove(&client_id) {
                let _ = tx.send(message);
            } else {
                // Store in Redis if no active connection
                let _: Result<String,()> = state.redis.lock().await
                    .rpush(format!("messages:{client_id}"), &message)
                    .await
                    .map_err(|_| ());
            }
        }
    }
}