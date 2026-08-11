#![cfg(feature = "neon")]

use std::sync::{Arc, Mutex};

use axum::{extract::State, http::HeaderMap, routing::post, Json, Router};
use dactyl_db::{Connection, Datastore, DatastoreRoute, Operation, OperationResult, Parameter};
use serde_json::{json, Value};
use tokio::runtime::Runtime;
use tokio::sync::oneshot;

type RequestLog = Arc<Mutex<Vec<(Value, Option<String>)>>>;

#[derive(Clone)]
struct MockState(RequestLog);

async fn query(
    State(state): State<MockState>,
    headers: HeaderMap,
    Json(request): Json<Value>,
) -> Json<Value> {
    let authorization = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    state
        .0
        .lock()
        .unwrap()
        .push((request.clone(), authorization));
    let sql = request["sql"].as_str().unwrap_or_default();
    if sql.starts_with("select") {
        Json(json!({
            "columns": ["id", "name", "enabled", "payload"],
            "rows": [{"id": 1, "name": "opened", "enabled": true, "payload": [1, 2, 3]}]
        }))
    } else {
        Json(json!({"affected_rows": 1, "rows": []}))
    }
}

async fn batch(
    State(state): State<MockState>,
    headers: HeaderMap,
    Json(request): Json<Value>,
) -> Json<Value> {
    let authorization = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    state
        .0
        .lock()
        .unwrap()
        .push((request.clone(), authorization));
    Json(json!({
        "results": [
            {"affected_rows": 0},
            {"affected_rows": 1, "generated_keys": [7]},
            {"columns": ["id"], "rows": [{"id": 7}]}
        ]
    }))
}

fn with_server(test: impl FnOnce(String, RequestLog) + Send + 'static) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    listener.set_nonblocking(true).unwrap();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let state = MockState(requests.clone());
    let (shutdown, shutdown_rx) = oneshot::channel();
    let thread = std::thread::spawn(move || {
        let runtime = Runtime::new().unwrap();
        runtime.block_on(async move {
            let app = Router::new()
                .route("/query", post(query))
                .route("/batch", post(batch))
                .with_state(state);
            axum::serve(tokio::net::TcpListener::from_std(listener).unwrap(), app)
                .with_graceful_shutdown(async {
                    let _ = shutdown_rx.await;
                })
                .await
                .unwrap();
        });
    });

    test(format!("http://{address}"), requests);
    let _ = shutdown.send(());
    thread.join().unwrap();
}

#[test]
fn neon_atomic_batch_preserves_results_and_explicit_keys() {
    with_server(|endpoint, requests| {
        let db =
            Connection::open(DatastoreRoute::neon(endpoint, Some("batch-token".into()))).unwrap();
        let result = db
            .atomic(&[
                Operation::schema("create table app (id integer primary key)", Vec::new()),
                Operation::write("insert into app default values", Vec::new()),
                Operation::read("select id from app", Vec::new()),
            ])
            .unwrap();
        assert!(matches!(result.results[0], OperationResult::Write(_)));
        match &result.results[1] {
            OperationResult::Write(result) => assert_eq!(result.generated_keys.len(), 1),
            other => panic!("unexpected result: {other:?}"),
        }
        match &result.results[2] {
            OperationResult::Rows(rows) => assert_eq!(rows.as_slice()[0].get_int("id").unwrap(), 7),
            other => panic!("unexpected result: {other:?}"),
        }
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].0["operations"][0]["kind"], "schema");
        assert_eq!(requests[0].1.as_deref(), Some("Bearer batch-token"));
    });
}

#[test]
fn neon_matches_the_application_read_write_shape() {
    with_server(|endpoint, requests| {
        let db =
            Connection::open(DatastoreRoute::neon(endpoint, Some("test-token".into()))).unwrap();
        assert_eq!(db.datastore(), Datastore::Neon);

        assert_eq!(
            db.write(
                "update app set name = $1 where id = $2",
                &[Parameter::Text("opened".into()), Parameter::Integer(1)],
            )
            .unwrap(),
            1
        );
        let rows = db
            .read("select id, name, enabled, payload from app", &[])
            .unwrap();
        let row = &rows.as_slice()[0];
        assert_eq!(row.get_int("id").unwrap(), 1);
        assert_eq!(row.get_str("name").unwrap(), "opened");
        assert!(row.get_bool("enabled").unwrap());
        assert_eq!(row.get_json("payload").unwrap(), json!([1, 2, 3]));

        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(
            requests[0].0["sql"],
            "update app set name = $1 where id = $2"
        );
        assert_eq!(requests[0].0["params"], json!(["opened", 1]));
        assert_eq!(requests[0].1.as_deref(), Some("Bearer test-token"));
        assert_eq!(requests[1].0["params"], json!([]));
    });
}
