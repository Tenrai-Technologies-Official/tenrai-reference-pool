use super::super::{NodeClient, NodeError};
use super::*;
use axum::{
    Json, Router,
    body::Body,
    extract::State,
    http::{StatusCode, Uri},
    response::Response,
    routing::get,
};
use libcrux_ml_dsa::ml_dsa_65::MLDSA65SigningKey;
use reqwest::header::HeaderValue;
use std::sync::Arc;
use tokio::net::TcpListener;
use url::Url;

const CHAIN: Hash384 = Hash384([0x42; 48]);

fn key_pair(seed: u8) -> (MLDSA65SigningKey, [u8; ML_DSA65_PUBLIC_KEY_BYTES]) {
    let mut signing = MLDSA65SigningKey::zero();
    let mut public_key = [0_u8; ML_DSA65_PUBLIC_KEY_BYTES];
    ml_dsa_65::portable::generate_key_pair_mut([seed; 32], signing.as_ref_mut(), &mut public_key);
    (signing, public_key)
}

fn trust(seed: u8) -> (NodeTrust, MLDSA65SigningKey) {
    let (signing, public_key) = key_pair(seed);
    let identity = node_identity(&public_key);
    let document = serde_json::json!({
        "api_version": 1,
        "network": "development",
        "chain_id": CHAIN.to_hex(),
        "identity": hex::encode(identity),
        "public_key": hex::encode(public_key),
        "key_scheme": "mldsa65",
    });
    let trust =
        NodeTrust::from_identity_document(&serde_json::to_vec(&document).unwrap(), CHAIN, identity)
            .unwrap();
    (trust, signing)
}

#[allow(clippy::too_many_arguments)]
fn signed_headers(
    trust: &NodeTrust,
    signing: &MLDSA65SigningKey,
    method: &str,
    target: &str,
    request: &[u8],
    status: u16,
    response: &[u8],
    nonce: [u8; REQUEST_NONCE_BYTES],
) -> HeaderMap {
    let message = envelope_bytes(
        CHAIN,
        method,
        target,
        digest(request),
        status,
        digest(response),
        trust.identity,
        nonce,
        47,
    )
    .unwrap();
    let signature = ml_dsa_65::portable::sign(signing, &message, SIGNING_CONTEXT, [7; 32]).unwrap();
    let mut headers = HeaderMap::new();
    headers.insert(HEADER_VERSION, HeaderValue::from_static("1"));
    headers.insert(
        HEADER_IDENTITY,
        HeaderValue::from_str(&hex::encode(trust.identity)).unwrap(),
    );
    headers.insert(
        HEADER_NONCE,
        HeaderValue::from_str(&hex::encode(nonce)).unwrap(),
    );
    headers.insert(HEADER_HEIGHT, HeaderValue::from_static("47"));
    headers.insert(
        HEADER_SIGNATURE,
        HeaderValue::from_str(&hex::encode(signature.as_ref())).unwrap(),
    );
    headers
}

#[test]
fn response_envelopes_bind_the_entire_http_exchange() {
    let (trust, signing) = trust(1);
    let nonce = [9; REQUEST_NONCE_BYTES];
    let request = br#"{"payout":"trai1example"}"#;
    let response = br#"{"job_id":"abc"}"#;
    let headers = signed_headers(
        &trust,
        &signing,
        "POST",
        "/v1/mining/templates",
        request,
        200,
        response,
        nonce,
    );
    let call = SignedCall {
        method: "POST",
        target: "/v1/mining/templates",
        request_body: request,
        status: 200,
        response_body: response,
        headers: &headers,
        nonce,
    };
    trust.verify(call).unwrap();

    let changed_body = SignedCall {
        response_body: br#"{"job_id":"other"}"#,
        ..call
    };
    assert!(trust.verify(changed_body).is_err());
    let moved = SignedCall {
        target: "/v1/mining/candidates",
        ..call
    };
    assert!(trust.verify(moved).is_err());
    let changed_request = SignedCall {
        request_body: br#"{\"payout\":\"trai1other\"}"#,
        ..call
    };
    assert!(trust.verify(changed_request).is_err());
    let changed_status = SignedCall {
        status: 201,
        ..call
    };
    assert!(trust.verify(changed_status).is_err());
    let replay = SignedCall {
        nonce: [8; REQUEST_NONCE_BYTES],
        ..call
    };
    assert!(trust.verify(replay).is_err());
}

#[test]
fn an_identity_document_must_match_the_operator_pin() {
    let (_, public_key) = key_pair(2);
    let identity = node_identity(&public_key);
    let document = serde_json::json!({
        "api_version": 1,
        "network": "development",
        "chain_id": CHAIN.to_hex(),
        "identity": hex::encode(identity),
        "public_key": hex::encode(public_key),
        "key_scheme": "mldsa65",
    });
    let body = serde_json::to_vec(&document).unwrap();

    assert!(NodeTrust::from_identity_document(&body, CHAIN, identity).is_ok());
    assert!(matches!(
        NodeTrust::from_identity_document(&body, Hash384([0x43; 48]), identity),
        Err(NodeTrustError::ChainMismatch)
    ));
    assert!(matches!(
        NodeTrust::from_identity_document(&body, CHAIN, [0; NODE_IDENTITY_BYTES]),
        Err(NodeTrustError::IdentityMismatch)
    ));

    let mut substituted = public_key;
    substituted[0] ^= 1;
    let substituted = serde_json::json!({
        "api_version": 1,
        "network": "development",
        "chain_id": CHAIN.to_hex(),
        "identity": hex::encode(identity),
        "public_key": hex::encode(substituted),
        "key_scheme": "mldsa65",
    });
    assert!(matches!(
        NodeTrust::from_identity_document(
            &serde_json::to_vec(&substituted).unwrap(),
            CHAIN,
            identity,
        ),
        Err(NodeTrustError::IdentityDocument)
    ));
}

#[test]
fn noncanonical_envelope_fields_are_rejected() {
    let (trust, signing) = trust(3);
    let nonce = [4; REQUEST_NONCE_BYTES];
    let mut headers = signed_headers(
        &trust,
        &signing,
        "GET",
        "/v1/node/info",
        b"",
        200,
        br#"{}"#,
        nonce,
    );
    headers.insert(HEADER_HEIGHT, HeaderValue::from_static("047"));
    let call = SignedCall {
        method: "GET",
        target: "/v1/node/info",
        request_body: b"",
        status: 200,
        response_body: br#"{}"#,
        headers: &headers,
        nonce,
    };
    assert!(matches!(trust.verify(call), Err(NodeTrustError::Unsigned)));
}

#[test]
fn duplicate_security_headers_are_rejected() {
    let (trust, signing) = trust(4);
    let nonce = [5; REQUEST_NONCE_BYTES];
    let mut headers = signed_headers(
        &trust,
        &signing,
        "GET",
        "/v1/node/info",
        b"",
        200,
        br#"{}"#,
        nonce,
    );
    headers.append(HEADER_NONCE, HeaderValue::from_static("00"));
    let call = SignedCall {
        method: "GET",
        target: "/v1/node/info",
        request_body: b"",
        status: 200,
        response_body: br#"{}"#,
        headers: &headers,
        nonce,
    };
    assert!(matches!(trust.verify(call), Err(NodeTrustError::Unsigned)));
}

struct TestNode {
    signing: MLDSA65SigningKey,
    public_key: [u8; ML_DSA65_PUBLIC_KEY_BYTES],
    identity: [u8; NODE_IDENTITY_BYTES],
    signed: bool,
}

async fn serve_test_node(signed: bool) -> (Url, Arc<TestNode>) {
    let (signing, public_key) = key_pair(8);
    let identity = node_identity(&public_key);
    let node = Arc::new(TestNode {
        signing,
        public_key,
        identity,
        signed,
    });
    let router = Router::new()
        .route("/v1/node/identity", get(identity_document))
        .route("/v1/node/info", get(node_info))
        .with_state(Arc::clone(&node));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    (Url::parse(&format!("http://{address}")).unwrap(), node)
}

async fn identity_document(State(node): State<Arc<TestNode>>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "api_version": 1,
        "network": "development",
        "chain_id": CHAIN.to_hex(),
        "identity": hex::encode(node.identity),
        "public_key": hex::encode(node.public_key),
        "key_scheme": "mldsa65",
    }))
}

async fn node_info(State(node): State<Arc<TestNode>>, headers: HeaderMap, uri: Uri) -> Response {
    let body = serde_json::to_vec(&serde_json::json!({
        "network": "development",
        "chain_id": CHAIN.to_hex(),
        "node_version": "test",
        "finalized": {"height": 47},
    }))
    .unwrap();
    let mut response = Response::new(Body::from(body.clone()));
    *response.status_mut() = StatusCode::OK;
    if !node.signed {
        return response;
    }
    let nonce: [u8; REQUEST_NONCE_BYTES] = headers
        .get(HEADER_NONCE)
        .and_then(|value| value.to_str().ok())
        .and_then(decode_lower)
        .expect("the client sends a canonical nonce");
    let target = uri
        .path_and_query()
        .map_or_else(|| uri.path(), |target| target.as_str());
    let message = envelope_bytes(
        CHAIN,
        "GET",
        target,
        digest(b""),
        200,
        digest(&body),
        node.identity,
        nonce,
        47,
    )
    .unwrap();
    let signature =
        ml_dsa_65::portable::sign(&node.signing, &message, SIGNING_CONTEXT, [5; 32]).unwrap();
    let headers = response.headers_mut();
    headers.insert(HEADER_VERSION, HeaderValue::from_static("1"));
    headers.insert(
        HEADER_IDENTITY,
        HeaderValue::from_str(&hex::encode(node.identity)).unwrap(),
    );
    headers.insert(
        HEADER_NONCE,
        HeaderValue::from_str(&hex::encode(nonce)).unwrap(),
    );
    headers.insert(HEADER_HEIGHT, HeaderValue::from_static("47"));
    headers.insert(
        HEADER_SIGNATURE,
        HeaderValue::from_str(&hex::encode(signature.as_ref())).unwrap(),
    );
    response
}

#[tokio::test]
async fn a_pinned_client_requires_a_signed_operational_response() {
    let (url, node) = serve_test_node(true).await;
    let mut client = NodeClient::new(url.clone(), url).unwrap();
    client.establish_trust(CHAIN, node.identity).await.unwrap();

    let info = client.info().await.unwrap();
    assert_eq!(info.finalized.height, 47);
}

#[tokio::test]
async fn a_pinned_client_rejects_an_unsigned_operational_response() {
    let (url, node) = serve_test_node(false).await;
    let mut client = NodeClient::new(url.clone(), url).unwrap();
    client.establish_trust(CHAIN, node.identity).await.unwrap();

    assert!(matches!(
        client.info().await,
        Err(NodeError::Trust(NodeTrustError::Unsigned))
    ));
}
