use super::{frame::next_frame, *};
use crate::stratum::protocol::{
    ClientMessage, JobNotice, PROTOCOL as V1, RejectReason, ServerMessage, SubscribeResult,
};
use bytes::BytesMut;
use serde_json::{Value, json};
use tokio_util::codec::{Decoder, Encoder};

fn job() -> Job {
    Job {
        channel_id: CHANNEL,
        job_id: [0xab; 48],
        header: vec![7; 312],
        share_target: [0x0f; 32],
        target: [0x01; 32],
        registration_height: 42,
        network: "development".into(),
        clean: true,
    }
}

fn notice() -> JobNotice {
    let job = job();
    JobNotice {
        job_id: hex::encode(job.job_id),
        header: hex::encode(&job.header),
        share_target: hex::encode(job.share_target),
        target: hex::encode(job.target),
        registration_height: job.registration_height,
        network: job.network,
        clean: job.clean,
        single_block: false,
    }
}

fn every_message() -> Vec<Message> {
    vec![
        Message::SetupConnection {
            pow_algorithm_id: crate::pow::tenq::ALGORITHM_ID,
            spec_revision: crate::pow::tenq::SPEC_REVISION,
            agent: "tenrai-miner/0.1.0".into(),
            tag: "rig one".into(),
        },
        Message::SetupConnectionSuccess {
            pow_algorithm_id: crate::pow::tenq::ALGORITHM_ID,
            spec_revision: crate::pow::tenq::SPEC_REVISION,
            chain_id: [0x44; 48],
            nonce_prefix: vec![1, 2],
            pool_address: "dtrai1pool".into(),
        },
        Message::SetupConnectionError {
            code: 28,
            message: "the miner tag is not allowed".into(),
        },
        Message::Reconnect {
            host: String::new(),
            port: 0,
        },
        Message::OpenChannel {
            request_id: 2,
            login: "dtrai1miner.rig1".into(),
            password: "d=4096".into(),
        },
        Message::OpenChannelSuccess {
            request_id: 2,
            channel_id: CHANNEL,
        },
        Message::OpenChannelError {
            request_id: 2,
            code: 27,
            message: "no address".into(),
        },
        Message::NewJob(job()),
        Message::SubmitShare {
            channel_id: CHANNEL,
            sequence_number: 9,
            job_id: [0xab; 48],
            nonce: [0x11; 32],
        },
        Message::SubmitSharesSuccess {
            channel_id: CHANNEL,
            last_sequence_number: 9,
            accepted: 3,
        },
        Message::SubmitSharesError {
            channel_id: CHANNEL,
            sequence_number: 9,
            code: 21,
            message: "job not found or stale".into(),
        },
        Message::SetTarget {
            channel_id: CHANNEL,
            share_target: [0xff; 32],
        },
        Message::SingleBlockJob {
            channel_id: CHANNEL,
            job_id: [0xab; 48],
        },
    ]
}

fn encoded(message: &Message) -> BytesMut {
    let mut bytes = BytesMut::new();
    message.encode(&mut bytes).unwrap();
    bytes
}

fn decoded(bytes: &mut BytesMut) -> Result<Option<Message>, CodecError> {
    let frame = next_frame(bytes)?.expect("a whole frame");
    Message::decode(&frame)
}

fn raw(kind: u8, channel: bool, payload: &[u8]) -> BytesMut {
    let mut bytes = BytesMut::new();
    frame::write_frame(&mut bytes, kind, channel, payload).unwrap();
    bytes
}

fn response(id: Value, outcome: Result<Value, (i64, String)>) -> ClientEvent {
    ClientEvent::Message(ServerMessage::Response { id, outcome })
}

#[test]
fn every_message_survives_a_round_trip() {
    for message in every_message() {
        assert_eq!(decoded(&mut encoded(&message)).unwrap(), Some(message));
    }
}

#[test]
fn frames_follow_the_stratum_v2_header_layout() {
    let bytes = encoded(&Message::SetTarget {
        channel_id: 1,
        share_target: [0xff; 32],
    });
    assert_eq!(&bytes[..10], &[0x00, 0x80, 0x21, 36, 0, 0, 1, 0, 0, 0]);
    assert_eq!(&bytes[10..], &[0xff; 32]);
    let setup = encoded(&Message::SetupConnection {
        pow_algorithm_id: crate::pow::tenq::ALGORITHM_ID,
        spec_revision: crate::pow::tenq::SPEC_REVISION,
        agent: "a".into(),
        tag: String::new(),
    });
    assert_eq!(
        &setup[..],
        &[0x00, 0x00, 0x00, 8, 0, 0, 1, b'a', 0, 3, 9, 0, 0, 0]
    );
}

#[test]
fn a_frame_waits_for_all_of_its_bytes() {
    let whole = encoded(&Message::NewJob(job()));
    let mut partial = BytesMut::new();
    for byte in &whole[..whole.len() - 1] {
        partial.extend_from_slice(&[*byte]);
        assert!(next_frame(&mut partial).unwrap().is_none());
    }
    partial.extend_from_slice(&whole[whole.len() - 1..]);
    assert_eq!(decoded(&mut partial).unwrap(), Some(Message::NewJob(job())));
    assert!(partial.is_empty());
}

#[test]
fn odd_frames_are_malformed() {
    let mut oversized = BytesMut::from(&[0, 0, 0x00, 0x01, 0x40, 0x00][..]);
    assert!(matches!(
        next_frame(&mut oversized),
        Err(CodecError::Oversized(16_385))
    ));
    let mut extension = BytesMut::from(&[1, 0, 0x00, 0, 0, 0][..]);
    assert!(matches!(
        next_frame(&mut extension),
        Err(CodecError::Malformed(_))
    ));

    let mut trailing = encoded(&Message::Reconnect {
        host: String::new(),
        port: 0,
    });
    trailing[3] += 1;
    trailing.extend_from_slice(&[0]);
    let mut bad_bool = encoded(&Message::NewJob(job()));
    let last = bad_bool.len() - 1;
    bad_bool[last] = 2;
    let mut wrong_flag = raw(0x21, false, &[0; 36]);
    let mut bad_text = raw(0x04, false, &[2, 0xff, 0xfe, 0, 0]);
    for bytes in [&mut trailing, &mut bad_bool, &mut wrong_flag, &mut bad_text] {
        assert!(matches!(decoded(bytes), Err(CodecError::Malformed(_))));
    }
}

#[test]
fn text_over_255_bytes_is_refused_before_it_leaves() {
    let message = Message::SetupConnection {
        pow_algorithm_id: crate::pow::tenq::ALGORITHM_ID,
        spec_revision: crate::pow::tenq::SPEC_REVISION,
        agent: "x".repeat(256),
        tag: String::new(),
    };
    assert!(message.encode(&mut BytesMut::new()).is_err());
}

#[test]
fn miners_skip_unknown_messages_and_servers_refuse_them() {
    let mut unknown = raw(0x7f, false, &[1, 2, 3]);
    unknown.extend_from_slice(&encoded(&Message::Reconnect {
        host: "pool.example".into(),
        port: 28742,
    }));
    assert_eq!(
        ClientCodec::default().decode(&mut unknown).unwrap(),
        Some(ClientEvent::Reconnect {
            host: "pool.example".into(),
            port: 28742
        })
    );
    let mut unknown = raw(0x7f, false, &[1, 2, 3]);
    assert!(ServerCodec::default().decode(&mut unknown).is_err());
}

#[test]
fn the_server_codec_speaks_v1_messages_to_the_mining_logic() {
    let mut server = ServerCodec::default();
    let mut input = encoded(&every_message()[0]);
    input.extend_from_slice(&encoded(&every_message()[4]));
    assert_eq!(
        server.decode(&mut input).unwrap(),
        Some(ClientMessage::Subscribe {
            pow_algorithm_id: crate::pow::tenq::ALGORITHM_ID,
            spec_revision: crate::pow::tenq::SPEC_REVISION,
            id: json!("setup"),
            agent: "tenrai-miner/0.1.0".into(),
            tag: "rig one".into(),
        })
    );
    assert_eq!(
        server.decode(&mut input).unwrap(),
        Some(ClientMessage::Authorize {
            id: json!("open"),
            login: "dtrai1miner.rig1".into(),
            password: "d=4096".into(),
        })
    );

    let result = SubscribeResult {
        pow_algorithm_id: crate::pow::tenq::ALGORITHM_ID,
        spec_revision: crate::pow::tenq::SPEC_REVISION,
        chain_id: hex::encode([0x44; 48]),
        protocol: V1.into(),
        nonce_prefix: "0102".into(),
        pool_address: String::new(),
    };
    let answers = [
        ServerMessage::reply(json!("setup"), serde_json::to_value(result).unwrap()),
        ServerMessage::reply(json!("open"), Value::Bool(true)),
        ServerMessage::SetTarget(hex::encode([0xff; 32])),
        ServerMessage::Notify(notice()),
    ];
    let mut output = BytesMut::new();
    for answer in answers {
        server.encode(answer, &mut output).unwrap();
    }
    let expected = [
        Message::SetupConnectionSuccess {
            pow_algorithm_id: crate::pow::tenq::ALGORITHM_ID,
            spec_revision: crate::pow::tenq::SPEC_REVISION,
            chain_id: [0x44; 48],
            nonce_prefix: vec![1, 2],
            pool_address: String::new(),
        },
        Message::OpenChannelSuccess {
            request_id: 2,
            channel_id: CHANNEL,
        },
        Message::SetTarget {
            channel_id: CHANNEL,
            share_target: [0xff; 32],
        },
        Message::NewJob(job()),
    ];
    for message in expected {
        assert_eq!(decoded(&mut output).unwrap(), Some(message));
    }

    let mut share = encoded(&every_message()[8]);
    assert_eq!(
        server.decode(&mut share).unwrap(),
        Some(ClientMessage::Submit {
            id: json!(9),
            job_id: "ab".repeat(48),
            nonce: "11".repeat(32),
        })
    );
    server
        .encode(
            ServerMessage::reply(json!(9), Value::Bool(true)),
            &mut output,
        )
        .unwrap();
    server
        .encode(
            ServerMessage::reject(json!(10), RejectReason::StaleJob),
            &mut output,
        )
        .unwrap();
    assert_eq!(
        decoded(&mut output).unwrap(),
        Some(Message::SubmitSharesSuccess {
            channel_id: CHANNEL,
            last_sequence_number: 9,
            accepted: 1,
        })
    );
    assert_eq!(
        decoded(&mut output).unwrap(),
        Some(Message::SubmitSharesError {
            channel_id: CHANNEL,
            sequence_number: 10,
            code: 21,
            message: "job not found or stale".into(),
        })
    );
}

#[test]
fn the_server_codec_holds_the_message_order() {
    let open = encoded(&every_message()[4]);
    let setup = encoded(&every_message()[0]);
    let share = encoded(&every_message()[8]);

    assert!(ServerCodec::default().decode(&mut open.clone()).is_err());
    assert!(ServerCodec::default().decode(&mut share.clone()).is_err());
    let mut server = ServerCodec::default();
    server.decode(&mut setup.clone()).unwrap();
    assert!(server.decode(&mut setup.clone()).is_err());
    let mut server = ServerCodec::default();
    server.decode(&mut setup.clone()).unwrap();
    server.decode(&mut open.clone()).unwrap();
    assert!(server.decode(&mut share.clone()).is_err());
    assert!(
        server
            .decode(&mut encoded(&Message::NewJob(job())))
            .is_err()
    );
}

#[test]
fn a_refused_channel_stays_closed() {
    let mut server = ServerCodec::default();
    server.decode(&mut encoded(&every_message()[0])).unwrap();
    server.decode(&mut encoded(&every_message()[4])).unwrap();
    let mut output = BytesMut::new();
    server
        .encode(
            ServerMessage::reject(json!("open"), RejectReason::InvalidAddress),
            &mut output,
        )
        .unwrap();
    assert_eq!(
        decoded(&mut output).unwrap(),
        Some(Message::OpenChannelError {
            request_id: 2,
            code: 27,
            message: RejectReason::InvalidAddress.message().into(),
        })
    );
    assert!(server.decode(&mut encoded(&every_message()[8])).is_err());
}

#[test]
fn the_client_codec_answers_in_v1_terms() {
    let mut client = ClientCodec::default();
    let mut output = BytesMut::new();
    client
        .encode(
            ClientMessage::Subscribe {
                pow_algorithm_id: crate::pow::tenq::ALGORITHM_ID,
                spec_revision: crate::pow::tenq::SPEC_REVISION,
                id: json!(1),
                agent: "tenrai-miner/0.1.0".into(),
                tag: String::new(),
            },
            &mut output,
        )
        .unwrap();
    client
        .encode(
            ClientMessage::Authorize {
                id: json!(2),
                login: "dtrai1miner.rig1".into(),
                password: String::new(),
            },
            &mut output,
        )
        .unwrap();
    let share = |id: u64| ClientMessage::Submit {
        id: json!(id),
        job_id: "ab".repeat(48),
        nonce: "11".repeat(32),
    };
    assert!(client.encode(share(3), &mut BytesMut::new()).is_err());

    let mut input = BytesMut::new();
    for message in [
        Message::SetupConnectionSuccess {
            pow_algorithm_id: crate::pow::tenq::ALGORITHM_ID,
            spec_revision: crate::pow::tenq::SPEC_REVISION,
            chain_id: [0x44; 48],
            nonce_prefix: vec![1, 2],
            pool_address: "dtrai1pool".into(),
        },
        Message::OpenChannelSuccess {
            request_id: 2,
            channel_id: CHANNEL,
        },
        Message::SetTarget {
            channel_id: CHANNEL,
            share_target: [0x0f; 32],
        },
        Message::NewJob(job()),
    ] {
        message.encode(&mut input).unwrap();
    }
    let subscribed = SubscribeResult {
        pow_algorithm_id: crate::pow::tenq::ALGORITHM_ID,
        spec_revision: crate::pow::tenq::SPEC_REVISION,
        chain_id: hex::encode([0x44; 48]),
        protocol: PROTOCOL.into(),
        nonce_prefix: "0102".into(),
        pool_address: "dtrai1pool".into(),
    };
    let expected = [
        response(json!(1), Ok(serde_json::to_value(subscribed).unwrap())),
        response(json!(2), Ok(Value::Bool(true))),
        ClientEvent::Message(ServerMessage::SetTarget("0f".repeat(32))),
        ClientEvent::Message(ServerMessage::Notify(notice())),
    ];
    for event in expected {
        assert_eq!(client.decode(&mut input).unwrap(), Some(event));
    }
    assert_eq!(client.decode(&mut input).unwrap(), None);

    for id in 3..=5 {
        client.encode(share(id), &mut output).unwrap();
    }
    for message in [
        Message::SubmitSharesError {
            channel_id: CHANNEL,
            sequence_number: 4,
            code: 23,
            message: "share above target".into(),
        },
        Message::SubmitSharesSuccess {
            channel_id: CHANNEL,
            last_sequence_number: 5,
            accepted: 2,
        },
    ] {
        message.encode(&mut input).unwrap();
    }
    let expected = [
        response(json!(4), Err((23, "share above target".into()))),
        response(json!(3), Ok(Value::Bool(true))),
        response(json!(5), Ok(Value::Bool(true))),
    ];
    for event in expected {
        assert_eq!(client.decode(&mut input).unwrap(), Some(event));
    }
    assert_eq!(client.decode(&mut input).unwrap(), None);
}

#[test]
fn a_whole_session_passes_through_both_codecs() {
    let (mut client, mut server) = (ClientCodec::default(), ServerCodec::default());
    let mut wire = BytesMut::new();
    let requests = [
        ClientMessage::Subscribe {
            pow_algorithm_id: crate::pow::tenq::ALGORITHM_ID,
            spec_revision: crate::pow::tenq::SPEC_REVISION,
            id: json!(1),
            agent: "tenrai-miner/0.1.0".into(),
            tag: "tag".into(),
        },
        ClientMessage::Authorize {
            id: json!(2),
            login: "dtrai1miner.rig1".into(),
            password: "x".into(),
        },
    ];
    for request in requests {
        client.encode(request, &mut wire).unwrap();
    }
    assert!(matches!(
        server.decode(&mut wire).unwrap(),
        Some(ClientMessage::Subscribe { .. })
    ));
    assert!(matches!(
        server.decode(&mut wire).unwrap(),
        Some(ClientMessage::Authorize { .. })
    ));

    let result = SubscribeResult {
        pow_algorithm_id: crate::pow::tenq::ALGORITHM_ID,
        spec_revision: crate::pow::tenq::SPEC_REVISION,
        chain_id: hex::encode([0x44; 48]),
        protocol: V1.into(),
        nonce_prefix: "beef".into(),
        pool_address: String::new(),
    };
    let answers = [
        ServerMessage::reply(json!("setup"), serde_json::to_value(result).unwrap()),
        ServerMessage::reply(json!("open"), Value::Bool(true)),
        ServerMessage::Notify(notice()),
    ];
    for answer in answers {
        server.encode(answer, &mut wire).unwrap();
    }
    let Some(ClientEvent::Message(ServerMessage::Response {
        id,
        outcome: Ok(result),
    })) = client.decode(&mut wire).unwrap()
    else {
        panic!("no setup answer");
    };
    let result: SubscribeResult = serde_json::from_value(result).unwrap();
    assert_eq!(
        (id, result.protocol.as_str(), result.nonce_prefix.as_str()),
        (json!(1), PROTOCOL, "beef")
    );
    assert_eq!(
        client.decode(&mut wire).unwrap(),
        Some(response(json!(2), Ok(Value::Bool(true))))
    );
    assert_eq!(
        client.decode(&mut wire).unwrap(),
        Some(ClientEvent::Message(ServerMessage::Notify(notice())))
    );

    client
        .encode(
            ClientMessage::Submit {
                id: json!(3),
                job_id: notice().job_id,
                nonce: "22".repeat(32),
            },
            &mut wire,
        )
        .unwrap();
    let Some(ClientMessage::Submit {
        id, job_id, nonce, ..
    }) = server.decode(&mut wire).unwrap()
    else {
        panic!("no share");
    };
    assert_eq!((job_id, nonce), (notice().job_id, "22".repeat(32)));
    server
        .encode(
            ServerMessage::reject(id, RejectReason::Duplicate),
            &mut wire,
        )
        .unwrap();
    assert_eq!(
        client.decode(&mut wire).unwrap(),
        Some(response(json!(3), Err((22, "duplicate share".into()))))
    );
}

#[test]
fn a_single_block_job_is_announced_first_and_reaches_the_miner() {
    let single = JobNotice {
        single_block: true,
        ..notice()
    };
    let mut wire = BytesMut::new();
    ServerCodec::default()
        .encode(ServerMessage::Notify(single.clone()), &mut wire)
        .unwrap();
    let mut frames = wire.clone();
    assert_eq!(
        decoded(&mut frames).unwrap(),
        Some(Message::SingleBlockJob {
            channel_id: CHANNEL,
            job_id: job().job_id,
        })
    );
    assert_eq!(decoded(&mut frames).unwrap(), Some(Message::NewJob(job())));

    let mut client = ClientCodec::default();
    let mut output = BytesMut::new();
    client
        .encode(
            ClientMessage::Subscribe {
                pow_algorithm_id: crate::pow::tenq::ALGORITHM_ID,
                spec_revision: crate::pow::tenq::SPEC_REVISION,
                id: json!(1),
                agent: "tenrai-miner/0.1.0".into(),
                tag: String::new(),
            },
            &mut output,
        )
        .unwrap();
    client
        .encode(
            ClientMessage::Authorize {
                id: json!(2),
                login: "dtrai1miner.rig1".into(),
                password: String::new(),
            },
            &mut output,
        )
        .unwrap();
    let mut input = BytesMut::new();
    for message in [
        Message::SetupConnectionSuccess {
            pow_algorithm_id: crate::pow::tenq::ALGORITHM_ID,
            spec_revision: crate::pow::tenq::SPEC_REVISION,
            chain_id: [0x44; 48],
            nonce_prefix: vec![1, 2],
            pool_address: String::new(),
        },
        Message::OpenChannelSuccess {
            request_id: 2,
            channel_id: CHANNEL,
        },
    ] {
        message.encode(&mut input).unwrap();
    }
    input.extend_from_slice(&wire);
    let mut events = Vec::new();
    while let Some(event) = client.decode(&mut input).unwrap() {
        events.push(event);
    }
    assert_eq!(
        events.last(),
        Some(&ClientEvent::Message(ServerMessage::Notify(single)))
    );
}

#[test]
fn legacy_setup_requests_and_answers_are_not_mining_identity_handshakes() {
    let mut server = ServerCodec::default();
    assert!(server.decode(&mut raw(0x00, false, &[1, b'a', 0])).is_err());
    assert!(
        server
            .decode(&mut encoded(&Message::OpenChannel {
                request_id: 1,
                login: "dtrai1miner.rig".into(),
                password: String::new(),
            }))
            .is_err()
    );
    assert!(
        ClientCodec::default()
            .decode(&mut raw(0x01, false, &[0, 0]))
            .is_err()
    );
}

#[test]
fn setup_codecs_preserve_the_claimed_identity_without_substituting_local_values() {
    let mut server = ServerCodec::default();
    let mut request = encoded(&Message::SetupConnection {
        agent: "foreign-miner".into(),
        tag: String::new(),
        pow_algorithm_id: 0xfe,
        spec_revision: 7,
    });
    assert_eq!(
        server.decode(&mut request).unwrap(),
        Some(ClientMessage::Subscribe {
            id: json!("setup"),
            agent: "foreign-miner".into(),
            tag: String::new(),
            pow_algorithm_id: 0xfe,
            spec_revision: 7,
        })
    );
    let mut answer = encoded(&Message::SetupConnectionSuccess {
        nonce_prefix: vec![1, 2],
        pool_address: "dtrai1pool".into(),
        pow_algorithm_id: 0xfe,
        spec_revision: 7,
        chain_id: [0xb4; 48],
    });
    assert_eq!(
        &answer[answer.len() - 53..answer.len() - 48],
        &[0xfe, 7, 0, 0, 0]
    );
    let Some(ClientEvent::Message(ServerMessage::Response {
        outcome: Ok(result),
        ..
    })) = ClientCodec::default().decode(&mut answer).unwrap()
    else {
        panic!("a setup answer");
    };
    let result: SubscribeResult = serde_json::from_value(result).unwrap();
    assert_eq!(result.pow_algorithm_id, 0xfe);
    assert_eq!(result.spec_revision, 7);
    assert_eq!(result.chain_id, "b4".repeat(48));
}
