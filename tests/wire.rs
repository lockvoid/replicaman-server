use base64::{Engine as _, engine::general_purpose::STANDARD};
use replicaman_server::{ErrorCode, wire::*};
use serde_json::{Value, json};
use uuid::Uuid;

const EPOCH: &str = "f4c05d55-70c7-41d9-9868-c0582ae3a020";
const OP: &str = "13fb2e27-3bf1-4415-b5a4-904e37b6494c";
const CLIENT: &str = "6ce01259-3dd6-42d1-8c7c-8dabf52c010a";
const ATTEMPT: &str = "8b152c66-aaac-43a1-855c-3b4147fd942c";
const SNAPSHOT: &str = "306356b3-0a95-45ed-9eec-5156e42193a8";

fn fixtures() -> Value {
    serde_json::from_str(include_str!("../docs/fixtures/wire.json")).unwrap()
}
fn message() -> Value {
    fixtures()["message_hash"][0]["message"].clone()
}
fn push(message: Value) -> Value {
    json!({"stream":"projects/example", "epoch":EPOCH, "message":message})
}
fn pull() -> Value {
    json!({"stream":"catalog", "epoch":EPOCH, "cursor":"0"})
}
fn snapshot() -> Value {
    json!({"stream":"catalog", "epoch":EPOCH, "client":CLIENT, "attempt":ATTEMPT, "page":null})
}
fn bytes(value: &Value) -> Vec<u8> {
    serde_json::to_vec(value).unwrap()
}
fn bad_event(value: Value) {
    assert_eq!(
        CloudEvent::from_value(value).unwrap_err().error,
        ErrorCode::BadRequest
    );
}

#[test]
fn portable_hash_and_base64_fixtures() {
    let fixture = fixtures();
    for case in fixture["message_hash"].as_array().unwrap() {
        let event = CloudEvent::from_value(case["message"].clone()).unwrap();
        assert_eq!(event.hash(), case["sha256"].as_str().unwrap());
        assert_eq!(
            String::from_utf8(event.canonical_header()).unwrap(),
            case["canonical_header"]
        );
        let decoded_hex: String = event
            .data()
            .unwrap()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        assert_eq!(decoded_hex, case["decoded_data_hex"]);
    }
    for case in fixture["base64"].as_array().unwrap() {
        let mut value = message();
        value["data_base64"] = case["encoded"].clone();
        let event = CloudEvent::from_value(value);
        assert_eq!(event.is_ok(), case["valid"].as_bool().unwrap());
        if let Ok(event) = event {
            let decoded_hex: String = event
                .data()
                .unwrap()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect();
            assert_eq!(decoded_hex, case["decoded_hex"]);
        }
    }
    assert_eq!(
        MAX_DATA_BYTES as u64,
        fixture["size"]["decoded_data_limit_bytes"]
    );
    assert_eq!(
        MAX_DATA_BYTES.div_ceil(3) as u64 * 4,
        fixture["size"]["base64_bytes_at_limit"]
    );
    assert_eq!(MAX_BODY_BYTES as u64, fixture["size"]["body_limit_bytes"]);
    assert_eq!(PAGE_BYTES, 1024 * 1024);
}

#[test]
fn hash_ignores_outer_order_whitespace_and_escaping_but_covers_attributes_and_payload() {
    let original = CloudEvent::from_value(message()).unwrap();
    let spelling = br#"{
        "data_base64":"e30=", "type":"example.change.v1", "source":"\u0075rn:example:client:example",
        "id":"13fb2e27-3bf1-4415-b5a4-904e37b6494c", "datacontenttype":"application\/json",
        "spec\u0076ersion":"1.0"
    }"#;
    let equivalent: CloudEvent = serde_json::from_slice(spelling).unwrap();
    assert_eq!(original.hash(), equivalent.hash());
    for (key, value) in [
        ("type", json!("example.other.v1")),
        ("extension", json!(true)),
        ("data_base64", json!(STANDARD.encode(b"{ }"))),
    ] {
        let mut changed = message();
        changed[key] = value;
        assert_ne!(
            original.hash(),
            CloudEvent::from_value(changed).unwrap().hash()
        );
    }
    let mut unicode = message();
    unicode["subject"] = json!("a\\b\"/שלום😀");
    unicode["min"] = json!(i32::MIN);
    unicode["max"] = json!(i32::MAX);
    let event = CloudEvent::from_value(unicode.clone()).unwrap();
    assert_eq!(serde_json::to_value(&event).unwrap(), unicode);
    assert_eq!(
        serde_json::from_slice::<CloudEvent>(&bytes(&unicode))
            .unwrap()
            .hash(),
        event.hash()
    );
}

#[test]
fn replacement_id_is_validated_and_does_not_mutate_the_original() {
    let original = CloudEvent::from_value(message()).unwrap();
    let id = Uuid::parse_str(SNAPSHOT).unwrap();
    let replacement = original.with_id(id).unwrap();
    assert_eq!(replacement.id(), id);
    assert_eq!(original.id().to_string(), OP);
    assert_eq!(replacement.data().unwrap(), original.data().unwrap());
    assert_ne!(replacement.hash(), original.hash());
    assert!(original.with_id(Uuid::nil()).is_err());
}

#[test]
fn duplicate_keys_are_rejected_after_unescaping_at_every_request_level() {
    let normal = String::from_utf8(bytes(&push(message()))).unwrap();
    let bodies = [
        normal.replacen(
            "\"stream\":",
            "\"stream\":\"projects/example\",\"\\u0073tream\":",
            1,
        ),
        normal.replacen(
            "\"specversion\":\"1.0\"",
            "\"specversion\":\"1.0\",\"spec\\u0076ersion\":\"1.0\"",
            1,
        ),
        normal.replacen(
            "\"data_base64\":\"e30=\"",
            "\"data_base64\":\"e30=\",\"data_base64\":\"e30=\"",
            1,
        ),
    ];
    for body in bodies {
        assert_eq!(
            parse_push(body.as_bytes()).unwrap_err().error,
            ErrorCode::BadRequest
        );
        assert!(serde_json::from_str::<PushRequest>(&body).is_err());
    }
    let cancel = format!(
        r#"{{"stream":"catalog","epoch":"{EPOCH}","cancel":{{"op":"{OP}","\u006fp":"{OP}","hash":"{}"}}}}"#,
        "0".repeat(64)
    );
    assert!(parse_push(cancel.as_bytes()).is_err());
    let nested = format!(
        r#"{{"stream":"catalog","epoch":"{EPOCH}","client":"{CLIENT}","attempt":"{ATTEMPT}","page":null,"pending":[{{"op":"{OP}","hash":"{}","h\u0061sh":"{}"}}]}}"#,
        "0".repeat(64),
        "0".repeat(64)
    );
    assert!(parse_snapshot(nested.as_bytes()).is_err());
    let duplicate_pull =
        format!(r#"{{"stream":"catalog","epoch":"{EPOCH}","cursor":"0","\u0063ursor":"0"}}"#);
    assert!(parse_pull(duplicate_pull.as_bytes()).is_err());
}

#[test]
fn malformed_json_utf8_surrogates_and_nonfinite_numbers_are_rejected() {
    let normal = String::from_utf8(bytes(&push(message()))).unwrap();
    for token in ["NaN", "Infinity", "-Infinity", "1e400", "-1e400"] {
        let body = normal.replacen(
            "\"specversion\":",
            &format!("\"x\":{token},\"specversion\":"),
            1,
        );
        assert_eq!(
            parse_push(body.as_bytes()).unwrap_err().error,
            ErrorCode::BadRequest
        );
    }
    for surrogate in [r#""\ud800""#, r#""\udfff""#, r#""\ud800x""#] {
        let body = normal.replacen(
            "\"specversion\":",
            &format!("\"x\":{surrogate},\"specversion\":"),
            1,
        );
        assert!(parse_push(body.as_bytes()).is_err());
    }
    let mut invalid_utf8 = bytes(&push(message()));
    invalid_utf8[2] = 0xff;
    assert!(parse_push(&invalid_utf8).is_err());
    for body in [
        b"{} true".as_slice(),
        b"{}\0",
        b"\xef\xbb\xbf{}",
        b"[]",
        b"null",
        b"{",
    ] {
        assert!(parse_push(body).is_err());
    }
    let mut valid_pair = message();
    valid_pair["x"] = json!("😀");
    assert!(CloudEvent::from_value(valid_pair).is_ok());
    // Payload JSON is opaque; outer duplicate-key rules do not parse it.
    let mut opaque = message();
    opaque["data_base64"] = json!(STANDARD.encode(br#"{"x":1,"x":2}"#));
    assert!(CloudEvent::from_value(opaque).is_ok());
}

#[test]
fn canonical_uuid_v4_is_required_for_all_uuid_fields() {
    let bad_ids = [
        OP.to_uppercase(),
        OP.replace('-', ""),
        format!("urn:uuid:{OP}"),
        format!("{{{OP}}}"),
        "13fb2e27-3bf1-1415-b5a4-904e37b6494c".into(),
        "13fb2e27-3bf1-4415-75a4-904e37b6494c".into(),
        Uuid::nil().to_string(),
    ];
    for id in bad_ids {
        let mut event = message();
        event["id"] = json!(id);
        bad_event(event);
        let mut request = pull();
        request["epoch"] = json!(id);
        assert!(parse_pull(&bytes(&request)).is_err());
        for field in ["client", "attempt"] {
            let mut request = snapshot();
            request[field] = json!(id);
            assert!(parse_snapshot(&bytes(&request)).is_err());
        }
        let mut request = snapshot();
        request["page"] = json!("token");
        request["snapshot"] = json!(id);
        assert!(parse_snapshot(&bytes(&request)).is_err());
        let request =
            json!({"stream":"catalog","epoch":EPOCH,"cancel":{"op":id,"hash":"0".repeat(64)}});
        assert!(parse_push(&bytes(&request)).is_err());
    }
}

#[test]
fn positions_are_strict_decimal_strings_with_signed_64_bit_bound() {
    for (text, value) in [("0", 0), ("1", 1), ("9223372036854775807", i64::MAX as u64)] {
        let position: Position = serde_json::from_value(json!(text)).unwrap();
        assert_eq!(position.0, value);
        assert_eq!(serde_json::to_value(position).unwrap(), json!(text));
    }
    for value in [
        json!(0),
        json!(-1),
        json!(1.0),
        json!(null),
        json!(""),
        json!("00"),
        json!("01"),
        json!("-0"),
        json!("+1"),
        json!("1.0"),
        json!("1e2"),
        json!(" 1"),
        json!("1 "),
        json!("١"),
        json!("9223372036854775808"),
        json!("18446744073709551616"),
    ] {
        assert!(
            serde_json::from_value::<Position>(value.clone()).is_err(),
            "{value}"
        );
        let mut request = pull();
        request["cursor"] = value;
        assert!(parse_pull(&bytes(&request)).is_err());
    }
    assert!(serde_json::to_value(Position(u64::MAX)).is_err());
}

#[test]
fn base64_requires_standard_alphabet_padding_and_zero_unused_bits() {
    for encoded in [
        "AA", "AA=", "AA===", "AB==", "AAB=", "A===", "A", "AA==\n", " AA==", "_w==", "-w==",
        "====",
    ] {
        let mut event = message();
        event["data_base64"] = json!(encoded);
        bad_event(event);
    }
    for (encoded, decoded) in [
        ("", vec![]),
        ("AA==", vec![0]),
        ("AAA=", vec![0, 0]),
        ("/w==", vec![255]),
        ("YWJj", b"abc".to_vec()),
    ] {
        let mut event = message();
        event["data_base64"] = json!(encoded);
        assert_eq!(
            CloudEvent::from_value(event).unwrap().data().unwrap(),
            decoded
        );
    }
}

#[test]
fn cloud_event_fields_and_extension_types_are_enforced() {
    for field in [
        "specversion",
        "id",
        "source",
        "type",
        "datacontenttype",
        "data_base64",
    ] {
        let mut event = message();
        event.as_object_mut().unwrap().remove(field);
        bad_event(event);
    }
    for field in [
        "specversion",
        "id",
        "source",
        "type",
        "datacontenttype",
        "data_base64",
        "subject",
        "time",
        "dataschema",
        "extension",
    ] {
        let mut event = message();
        event[field] = Value::Null;
        bad_event(event);
    }
    for (field, value) in [
        ("data", json!({})),
        ("x", json!([])),
        ("x", json!({})),
        ("x", json!(1.0)),
        ("x", json!(i64::from(i32::MAX) + 1)),
        ("x", json!(i64::from(i32::MIN) - 1)),
        ("x_1", json!(true)),
        ("X", json!(true)),
        ("1x", json!(true)),
        ("abcdefghijklmnopqrstu", json!(true)),
        ("subject", json!("")),
        ("type", json!("")),
        ("source", json!("")),
        ("datacontenttype", json!("")),
    ] {
        let mut event = message();
        event[field] = value;
        bad_event(event);
    }
    let mut event = message();
    event["specversion"] = json!("2.0");
    assert_eq!(
        CloudEvent::from_value(event).unwrap_err().error,
        ErrorCode::UnsupportedFormat
    );
    for text in [
        "line\nfeed",
        "\u{7f}",
        "\u{80}",
        "\u{fdd0}",
        "\u{ffff}",
        "\u{1fffe}",
    ] {
        let mut event = message();
        event["x"] = json!(text);
        bad_event(event);
    }
    let mut event = message();
    for (field, value) in [
        ("x", json!("")),
        ("x1", json!(false)),
        ("min", json!(i32::MIN)),
        ("max", json!(i32::MAX)),
        ("abcdefghijklmnopqrst", json!(true)),
    ] {
        event[field] = value;
    }
    assert!(CloudEvent::from_value(event).is_ok());
}

#[test]
fn standard_uri_media_type_and_timestamp_attributes_are_validated_without_rewriting() {
    for source in [
        "urn:example:example",
        "/events/123",
        "../item?x=y#part",
        "?query",
        "#fragment",
        "//example.test/path",
        "https://[::1]:443/x",
        "custom://[v1.a:b]/x",
        "https://example.test/%20",
    ] {
        let mut event = message();
        event["source"] = json!(source);
        assert_eq!(
            CloudEvent::from_value(event).unwrap().as_value()["source"],
            source
        );
    }
    for source in [
        "with space",
        "https://x/%",
        "https://x/%GG",
        "https://x/\\path",
        "https://x/[path]",
        "https://[not-ip]/",
        "https://x:abc/",
        "1bad:scheme",
        "x#first#second",
        "https://x/é",
    ] {
        let mut event = message();
        event["source"] = json!(source);
        bad_event(event);
    }
    for schema in ["", "/relative", "schema.json", "https://x/schema#fragment"] {
        let mut event = message();
        event["dataschema"] = json!(schema);
        bad_event(event);
    }
    for content_type in [
        "json",
        "application/",
        "text/plain\n",
        "*/*",
        "text/*",
        "application/json;broken",
    ] {
        let mut event = message();
        event["datacontenttype"] = json!(content_type);
        bad_event(event);
    }
    for time in [
        "2026-10-08T12:34:56Z",
        "2026-10-08t12:34:56z",
        "2026-10-08T12:34:56.123+03:00",
        "2016-12-31T23:59:60Z",
    ] {
        let mut event = message();
        event["time"] = json!(time);
        event["dataschema"] = json!("urn:schema:example");
        event["datacontenttype"] = json!("application/json; charset=utf-8");
        assert!(CloudEvent::from_value(event).is_ok(), "{time}");
    }
    for time in [
        "2026-10-08",
        "2026-10-08 12:34:56Z",
        "2026-02-30T12:34:56Z",
        "2026-10-08T12:34:56",
        "2026-10-08T12:34:56.Z",
        "2026-10-08T12:34:56+0300",
        "2026-10-08T12:34:56+25:00",
    ] {
        let mut event = message();
        event["time"] = json!(time);
        bad_event(event);
    }
}

#[test]
fn byte_limits_apply_to_canonical_header_and_utf8_fields() {
    let mut event = message();
    event["type"] = json!("é".repeat(127) + "a");
    assert!(CloudEvent::from_value(event.clone()).is_ok());
    event["type"] = json!("é".repeat(128));
    bad_event(event);
    let mut event = message();
    event["source"] = json!("x".repeat(1024));
    assert!(CloudEvent::from_value(event.clone()).is_ok());
    event["source"] = json!("x".repeat(1025));
    bad_event(event);

    let mut event = message();
    event["x"] = json!("");
    let base = CloudEvent::from_value(event.clone())
        .unwrap()
        .canonical_header()
        .len();
    event["x"] = json!("a".repeat(MAX_HEADER_BYTES - base));
    assert_eq!(
        CloudEvent::from_value(event.clone())
            .unwrap()
            .canonical_header()
            .len(),
        MAX_HEADER_BYTES
    );
    event["x"] = json!("a".repeat(MAX_HEADER_BYTES - base + 1));
    assert_eq!(
        CloudEvent::from_value(event).unwrap_err().error,
        ErrorCode::PayloadTooLarge
    );
}

#[test]
fn data_and_decompressed_body_limits_include_exact_boundaries() {
    let mut event = message();
    event["data_base64"] = json!(STANDARD.encode(vec![0; MAX_DATA_BYTES]));
    assert!(CloudEvent::from_value(event).is_ok());
    // The next decoded byte has the same encoded length for this limit.
    let mut event = message();
    event["data_base64"] = json!(STANDARD.encode(vec![0; MAX_DATA_BYTES + 1]));
    assert_eq!(
        CloudEvent::from_value(event).unwrap_err().error,
        ErrorCode::PayloadTooLarge
    );

    let mut body = bytes(&pull());
    body.resize(MAX_BODY_BYTES, b' ');
    assert!(parse_pull(&body).is_ok());
    body.push(b' ');
    assert_eq!(
        parse_pull(&body).unwrap_err().error,
        ErrorCode::PayloadTooLarge
    );
    assert_eq!(
        parse_push(&body).unwrap_err().error,
        ErrorCode::PayloadTooLarge
    );
    assert_eq!(
        parse_snapshot(&body).unwrap_err().error,
        ErrorCode::PayloadTooLarge
    );
}

#[test]
fn push_has_exactly_one_operation_and_no_unknown_fields() {
    let valid = parse_push(&bytes(&push(message()))).unwrap();
    assert!(matches!(valid.operation, PushOperation::Message(_)));
    assert_eq!(
        parse_push(&serde_json::to_vec(&valid).unwrap()).unwrap(),
        valid
    );
    let cancel = json!({"stream":"catalog","epoch":EPOCH,"cancel":{"op":OP,"hash":"0".repeat(64)}});
    let valid = parse_push(&bytes(&cancel)).unwrap();
    assert!(matches!(valid.operation, PushOperation::Cancel { .. }));
    assert_eq!(
        parse_push(&serde_json::to_vec(&valid).unwrap()).unwrap(),
        valid
    );
    for mut request in [push(message()), cancel.clone()] {
        request["extra"] = json!(true);
        assert!(parse_push(&bytes(&request)).is_err());
    }
    let mut both = push(message());
    both["cancel"] = cancel["cancel"].clone();
    assert!(parse_push(&bytes(&both)).is_err());
    let mut none = push(message());
    none.as_object_mut().unwrap().remove("message");
    assert!(parse_push(&bytes(&none)).is_err());
    let mut extra = cancel;
    extra["cancel"]["extra"] = json!(true);
    assert!(parse_push(&bytes(&extra)).is_err());
    for field in ["message", "cancel"] {
        let request = json!({"stream":"catalog","epoch":EPOCH,field:null});
        assert!(parse_push(&bytes(&request)).is_err());
    }
}

#[test]
fn pull_defaults_and_optional_fields_are_strict() {
    let request = parse_pull(&bytes(&pull())).unwrap();
    assert_eq!(request.limit, 100);
    assert_eq!(request.to, None);
    assert_eq!(
        parse_pull(&serde_json::to_vec(&request).unwrap()).unwrap(),
        request
    );
    let mut with_to = pull();
    with_to["to"] = json!(null);
    with_to["limit"] = json!(1);
    assert!(parse_pull(&bytes(&with_to)).is_ok());
    with_to["to"] = json!("20");
    assert_eq!(parse_pull(&bytes(&with_to)).unwrap().to, Some(Position(20)));
    for limit in [
        json!(null),
        json!(0),
        json!(101),
        json!(-1),
        json!(1.0),
        json!("1"),
        json!(true),
    ] {
        let mut request = pull();
        request["limit"] = limit;
        assert!(parse_pull(&bytes(&request)).is_err());
    }
    for field in ["pending", "client", "extra"] {
        let mut request = pull();
        request[field] = json!(null);
        assert!(parse_pull(&bytes(&request)).is_err());
    }
    let mut request = pull();
    request["to"] = json!(1);
    assert!(parse_pull(&bytes(&request)).is_err());
}

#[test]
fn snapshot_create_and_page_are_disjoint_and_pending_is_bounded_unique_and_ordered() {
    let create = parse_snapshot(&bytes(&snapshot())).unwrap();
    assert_eq!(
        create.kind,
        SnapshotKind::Create {
            limit: 100,
            pending: vec![]
        }
    );
    assert_eq!(
        parse_snapshot(&serde_json::to_vec(&create).unwrap()).unwrap(),
        create
    );
    let mut request = snapshot();
    request["pending"] =
        json!([{"op":OP,"hash":"0".repeat(64)}, {"op":SNAPSHOT,"hash":"1".repeat(64)}]);
    let parsed = parse_snapshot(&bytes(&request)).unwrap();
    match parsed.kind {
        SnapshotKind::Create { pending, .. } => {
            assert_eq!(pending[0].op.to_string(), OP);
            assert_eq!(pending[1].op.to_string(), SNAPSHOT);
        }
        _ => unreachable!(),
    }
    request["pending"][1]["op"] = json!(OP);
    assert!(parse_snapshot(&bytes(&request)).is_err());
    for field in ["snapshot", "extra"] {
        let mut request = snapshot();
        request[field] = json!(SNAPSHOT);
        assert!(parse_snapshot(&bytes(&request)).is_err());
    }
    let mut request = snapshot();
    request["pending"] = json!(null);
    assert!(parse_snapshot(&bytes(&request)).is_err());
    let mut request = snapshot();
    request.as_object_mut().unwrap().remove("page");
    assert!(parse_snapshot(&bytes(&request)).is_err());

    let pending: Vec<Value> = (0..100)
        .map(|_| json!({"op":Uuid::new_v4(),"hash":"0".repeat(64)}))
        .collect();
    let mut request = snapshot();
    request["pending"] = json!(pending);
    assert!(parse_snapshot(&bytes(&request)).is_ok());
    request["pending"]
        .as_array_mut()
        .unwrap()
        .push(json!({"op":Uuid::new_v4(),"hash":"0".repeat(64)}));
    assert!(parse_snapshot(&bytes(&request)).is_err());

    let mut page = snapshot();
    page["page"] = json!("opaque");
    page["snapshot"] = json!(SNAPSHOT);
    let parsed = parse_snapshot(&bytes(&page)).unwrap();
    assert!(matches!(
        parsed.kind,
        SnapshotKind::Page { limit: None, .. }
    ));
    assert_eq!(
        parse_snapshot(&serde_json::to_vec(&parsed).unwrap()).unwrap(),
        parsed
    );
    page["limit"] = json!(100);
    assert!(parse_snapshot(&bytes(&page)).is_ok());
    page["pending"] = json!([]);
    assert!(parse_snapshot(&bytes(&page)).is_err());
    page.as_object_mut().unwrap().remove("pending");
    for token in ["".to_string(), "x".repeat(2049), "é".into()] {
        page["page"] = json!(token);
        assert!(parse_snapshot(&bytes(&page)).is_err());
    }
    page["page"] = json!("x".repeat(2048));
    assert!(parse_snapshot(&bytes(&page)).is_ok());
    page["limit"] = json!(null);
    assert!(parse_snapshot(&bytes(&page)).is_err());
}

#[test]
fn typed_callers_cannot_bypass_request_constraints() {
    for stream in [
        "",
        ".catalog",
        "_catalog",
        "space here",
        "é",
        &"a".repeat(201),
    ] {
        assert!(validate_stream(stream).is_err());
    }
    assert!(validate_stream(&"a".repeat(200)).is_ok());
    assert!(validate_stream("a.b_c:/-012").is_ok());
    for hash in [
        "",
        &"0".repeat(63),
        &"0".repeat(65),
        &"A".repeat(64),
        &"g".repeat(64),
    ] {
        assert!(validate_hash(hash).is_err());
    }
    for code in ["", "_bad", "Bad", "bad-code", &"a".repeat(65)] {
        assert!(validate_code(code).is_err());
    }
    assert!(validate_code("application_error_1").is_ok());

    let mut request = parse_pull(&bytes(&pull())).unwrap();
    request.cursor = Position(u64::MAX);
    assert!(validate_pull(&request).is_err());
    request.cursor = Position(0);
    request.limit = 0;
    assert!(validate_pull(&request).is_err());
    let mut request = parse_push(&bytes(&push(message()))).unwrap();
    request.epoch = Uuid::nil();
    assert!(validate_push(&request).is_err());
    let mut request = parse_snapshot(&bytes(&snapshot())).unwrap();
    request.kind = SnapshotKind::Page {
        snapshot: Uuid::parse_str(SNAPSHOT).unwrap(),
        page: "é".into(),
        limit: None,
    };
    assert!(validate_snapshot(&request).is_err());
}
