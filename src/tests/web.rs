use super::super::protocol::ServerMessage;
use super::super::wire::{Inbound, Wire};
use super::*;
use futures_util::{SinkExt, StreamExt};
use tokio::io::{DuplexStream, duplex};
use tokio_tungstenite::{
    client_async,
    tungstenite::{
        Error, Message, client::IntoClientRequest, handshake::client::Request as ClientRequest,
    },
};

fn request(origin: Option<&str>, real_ip: Option<&str>) -> ClientRequest {
    let mut request = "ws://pool.test/".into_client_request().unwrap();
    if let Some(origin) = origin {
        request
            .headers_mut()
            .insert("origin", origin.parse().unwrap());
    }
    if let Some(address) = real_ip {
        request
            .headers_mut()
            .insert("x-real-ip", address.parse().unwrap());
    }
    request
}

async fn connect(
    origins: &WebOrigins,
    proxied: bool,
    request: ClientRequest,
) -> (
    Result<(WebSocketStream<DuplexStream>, Option<IpAddr>)>,
    std::result::Result<WebSocketStream<DuplexStream>, Error>,
) {
    let (client, server) = duplex(1 << 16);
    let (accepted, connected) = tokio::join!(
        accept(server, origins, proxied),
        client_async(request, client)
    );
    (accepted, connected.map(|(stream, _)| stream))
}

#[test]
fn origins_are_bare_and_compared_without_case() {
    let origins = WebOrigins::parse(&[
        "https://Miner.Example/".into(),
        "http://127.0.0.1:28782".into(),
    ])
    .unwrap();
    assert!(origins.admits(Some("https://miner.example")));
    assert!(origins.admits(Some("http://127.0.0.1:28782")));
    assert!(!origins.admits(Some("https://evil.example")));
    assert!(!origins.admits(Some("null")));
    assert!(origins.admits(None), "programs send no Origin");
    assert!(WebOrigins::default().admits(Some("https://anyone.example")));
    for bad in [
        "miner.example",
        "ftp://miner.example",
        "https://miner.example/path",
        "https://",
        "https://a b",
        "https://user@miner.example",
    ] {
        assert!(WebOrigins::parse(&[bad.into()]).is_err(), "{bad}");
    }
}

#[tokio::test]
async fn a_line_is_one_text_message_and_other_frames_count_against_the_page() {
    let (accepted, connected) = connect(
        &WebOrigins::default(),
        false,
        request(Some("https://page.example"), None),
    )
    .await;
    let (stream, forwarded) = accepted.unwrap();
    assert_eq!(forwarded, None);
    let mut client = connected.unwrap();
    let mut wire = Wire::Web(stream);

    client.send(Message::text(r#"{"id":1}"#)).await.unwrap();
    assert!(matches!(wire.next().await, Some(Ok(Inbound::Line(line))) if line == r#"{"id":1}"#));
    client.send(Message::Ping(vec![1].into())).await.unwrap();
    assert!(matches!(wire.next().await, Some(Ok(Inbound::Control))));
    client
        .send(Message::Binary(vec![1, 2].into()))
        .await
        .unwrap();
    assert!(matches!(wire.next().await, Some(Ok(Inbound::Junk(_)))));

    wire.send(ServerMessage::SetTarget("ff".repeat(32)))
        .await
        .unwrap();
    let answer = loop {
        match client.next().await.unwrap().unwrap() {
            Message::Text(text) => break text.as_str().to_owned(),
            _ => continue,
        }
    };
    assert!(answer.contains("mining.set_target"), "{answer}");

    client
        .send(Message::text("x".repeat(MAX_LINE_BYTES + 1)))
        .await
        .unwrap();
    assert!(matches!(
        wire.next().await,
        Some(Ok(Inbound::Junk(_))) | Some(Err(_)) | None
    ));
}

#[tokio::test]
async fn a_page_off_the_list_is_refused_and_only_a_local_proxy_names_the_page() {
    let origins = WebOrigins::parse(&["https://miner.example".into()]).unwrap();
    let (accepted, connected) =
        connect(&origins, false, request(Some("https://evil.example"), None)).await;
    assert!(accepted.is_err());
    assert!(matches!(connected, Err(Error::Http(response)) if response.status() == 403));

    let (accepted, connected) = connect(
        &origins,
        true,
        request(Some("https://miner.example"), Some("203.0.113.9")),
    )
    .await;
    assert_eq!(accepted.unwrap().1, Some("203.0.113.9".parse().unwrap()));
    assert!(connected.is_ok());

    let (accepted, _) = connect(
        &origins,
        false,
        request(Some("https://miner.example"), Some("203.0.113.9")),
    )
    .await;
    assert_eq!(
        accepted.unwrap().1,
        None,
        "a direct connection's headers name no address"
    );

    let (accepted, _) = connect(&origins, true, request(None, Some("not an address"))).await;
    assert_eq!(accepted.unwrap().1, None);
}
