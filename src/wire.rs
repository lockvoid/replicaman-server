//! The Replicaman v1 wire profile. Use `parse_*` for untrusted HTTP bodies.
//!
//! Duplicate detection happens while JSON keys are decoded, before a map can
//! discard them. Canonicalization covers only this profile's ASCII attribute
//! names and string/bool/i32 header values; it is not a general JCS encoder.

use crate::{Error, ErrorCode, Result};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::de::{self, MapAccess, SeqAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Map, Number, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::net::Ipv6Addr;
use uuid::{Uuid, Variant, Version};

pub const MAX_DATA_BYTES: usize = 32 * 1024 * 1024;
pub const MAX_BODY_BYTES: usize = 48 * 1024 * 1024;
pub const PAGE_BYTES: usize = 1024 * 1024;
pub const MAX_HEADER_BYTES: usize = 8 * 1024;
const MAX_BASE64_BYTES: usize = MAX_DATA_BYTES.div_ceil(3) * 4;
const DEFAULT_LIMIT: usize = 100;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Position(pub u64);

impl Position {
    pub fn validate(self) -> Result<()> {
        if self.0 <= i64::MAX as u64 {
            Ok(())
        } else {
            Err(Error::bad_request())
        }
    }
}

impl Serialize for Position {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        self.validate().map_err(serde::ser::Error::custom)?;
        serializer.serialize_str(&self.0.to_string())
    }
}

impl<'de> Deserialize<'de> for Position {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        parse_position(&text).map_err(de::Error::custom)
    }
}

fn parse_position(text: &str) -> Result<Position> {
    if text.is_empty()
        || text.len() > 19
        || !text.bytes().all(|b| b.is_ascii_digit())
        || (text.len() > 1 && text.starts_with('0'))
    {
        return Err(Error::bad_request());
    }
    let position = Position(text.parse().map_err(|_| Error::bad_request())?);
    position.validate()?;
    Ok(position)
}

/// A validated message whose attributes cannot be mutated without revalidation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloudEvent(Map<String, Value>);

impl CloudEvent {
    pub fn new(attributes: Map<String, Value>) -> Result<Self> {
        let event = Self(attributes);
        event.validate()?;
        Ok(event)
    }

    pub fn from_value(value: Value) -> Result<Self> {
        Self::new(object(value)?)
    }

    pub fn as_value(&self) -> Value {
        Value::Object(self.0.clone())
    }

    pub fn id(&self) -> Uuid {
        Uuid::parse_str(self.0["id"].as_str().expect("validated string id"))
            .expect("validated UUID")
    }

    pub fn with_id(&self, id: Uuid) -> Result<Self> {
        validate_uuid(id)?;
        let mut attributes = self.0.clone();
        attributes.insert("id".into(), Value::String(id.to_string()));
        Self::new(attributes)
    }

    pub fn data(&self) -> Result<Vec<u8>> {
        decode_data(required_string(&self.0, "data_base64")?)
    }

    /// JCS for this validated, flat header subset only.
    pub fn canonical_header(&self) -> Vec<u8> {
        let header: BTreeMap<&str, &Value> = self
            .0
            .iter()
            .filter(|(key, _)| key.as_str() != "data_base64")
            .map(|(key, value)| (key.as_str(), value))
            .collect();
        // ASCII keys have identical UTF-8/UTF-16 ordering. Integer values are
        // i32, and serde_json's string escaping matches JCS for valid strings.
        serde_json::to_vec(&header).expect("validated JSON header")
    }

    pub fn hash(&self) -> String {
        let mut hash = Sha256::new();
        hash.update(b"replicaman-message/1\0");
        hash.update(self.canonical_header());
        hash.update([0]);
        hash.update(self.data().expect("validated canonical Base64"));
        hash.finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }

    pub fn validate(&self) -> Result<()> {
        // Reject null and invalid CloudEvents strings before conversion/hash.
        for (name, value) in &self.0 {
            if value.is_null() {
                return Err(Error::bad_request());
            }
            if let Value::String(text) = value
                && !cloud_string(text)
            {
                return Err(Error::bad_request());
            }
            match name.as_str() {
                "specversion" | "id" | "source" | "type" | "datacontenttype" | "data_base64"
                | "subject" | "time" | "dataschema" => {
                    if !value.is_string() {
                        return Err(Error::bad_request());
                    }
                }
                "data" => return Err(Error::bad_request()),
                _ => {
                    if !extension_name(name) {
                        return Err(Error::bad_request());
                    }
                    match value {
                        Value::String(_) | Value::Bool(_) => {}
                        Value::Number(number)
                            if number.as_i64().is_some_and(|n| i32::try_from(n).is_ok()) => {}
                        _ => return Err(Error::bad_request()),
                    }
                }
            }
        }

        let version = required_string(&self.0, "specversion")?;
        if version.is_empty() {
            return Err(Error::bad_request());
        }
        if version != "1.0" {
            return Err(Error::new(ErrorCode::UnsupportedFormat));
        }
        parse_uuid(required_string(&self.0, "id")?)?;
        let source = bounded_string(&self.0, "source", 1024)?;
        if !uri_reference(source, false) {
            return Err(Error::bad_request());
        }
        bounded_string(&self.0, "type", 255)?;
        let content_type = bounded_string(&self.0, "datacontenttype", 255)?;
        let media: mime::Mime = content_type.parse().map_err(|_| Error::bad_request())?;
        if media.type_().as_str().is_empty()
            || media.subtype().as_str().is_empty()
            || media.type_() == mime::STAR
            || media.subtype() == mime::STAR
        {
            return Err(Error::bad_request());
        }
        if let Some(value) = self.0.get("subject")
            && value.as_str().is_none_or(str::is_empty)
        {
            return Err(Error::bad_request());
        }
        if let Some(value) = self.0.get("time")
            && !timestamp(value.as_str().ok_or_else(Error::bad_request)?)
        {
            return Err(Error::bad_request());
        }
        if let Some(value) = self.0.get("dataschema")
            && !uri_reference(value.as_str().ok_or_else(Error::bad_request)?, true)
        {
            return Err(Error::bad_request());
        }
        if self.canonical_header().len() > MAX_HEADER_BYTES {
            return Err(Error::new(ErrorCode::PayloadTooLarge));
        }
        self.data()?;
        Ok(())
    }
}

impl Serialize for CloudEvent {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for CloudEvent {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        Self::from_value(StrictValue::deserialize(deserializer)?.0).map_err(de::Error::custom)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PushRequest {
    pub stream: String,
    pub epoch: Uuid,
    pub operation: PushOperation,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PushOperation {
    Message(CloudEvent),
    Cancel { op: Uuid, hash: String },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct PullRequest {
    pub stream: String,
    pub epoch: Uuid,
    pub cursor: Position,
    pub to: Option<Position>,
    pub limit: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotRequest {
    pub stream: String,
    pub epoch: Uuid,
    pub client: Uuid,
    pub attempt: Uuid,
    pub kind: SnapshotKind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SnapshotKind {
    Create {
        limit: usize,
        pending: Vec<Pending>,
    },
    Page {
        snapshot: Uuid,
        page: String,
        limit: Option<usize>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Pending {
    pub op: Uuid,
    pub hash: String,
}

pub fn parse_push(bytes: &[u8]) -> Result<PushRequest> {
    push_value(parse_json(bytes)?)
}
pub fn parse_pull(bytes: &[u8]) -> Result<PullRequest> {
    pull_value(parse_json(bytes)?)
}
pub fn parse_snapshot(bytes: &[u8]) -> Result<SnapshotRequest> {
    snapshot_value(parse_json(bytes)?)
}

pub fn validate_push(request: &PushRequest) -> Result<()> {
    validate_stream(&request.stream)?;
    validate_uuid(request.epoch)?;
    match &request.operation {
        PushOperation::Message(message) => message.validate(),
        PushOperation::Cancel { op, hash } => {
            validate_uuid(*op)?;
            validate_hash(hash)
        }
    }
}

pub fn validate_pull(request: &PullRequest) -> Result<()> {
    validate_stream(&request.stream)?;
    validate_uuid(request.epoch)?;
    request.cursor.validate()?;
    if let Some(to) = request.to {
        to.validate()?;
    }
    validate_limit(request.limit)
}

pub fn validate_snapshot(request: &SnapshotRequest) -> Result<()> {
    validate_stream(&request.stream)?;
    validate_uuid(request.epoch)?;
    validate_uuid(request.client)?;
    validate_uuid(request.attempt)?;
    match &request.kind {
        SnapshotKind::Create { limit, pending } => {
            validate_limit(*limit)?;
            if pending.len() > 100 {
                return Err(Error::bad_request());
            }
            let mut seen = HashSet::new();
            for item in pending {
                validate_uuid(item.op)?;
                validate_hash(&item.hash)?;
                if !seen.insert(item.op) {
                    return Err(Error::bad_request());
                }
            }
        }
        SnapshotKind::Page {
            snapshot,
            page,
            limit,
        } => {
            validate_uuid(*snapshot)?;
            if page.is_empty() || page.len() > 2048 || !page.is_ascii() {
                return Err(Error::bad_request());
            }
            if let Some(limit) = limit {
                validate_limit(*limit)?;
            }
        }
    }
    Ok(())
}

pub fn validate_stream(stream: &str) -> Result<()> {
    let valid = !stream.is_empty()
        && stream.len() <= 200
        && stream.as_bytes()[0].is_ascii_alphanumeric()
        && stream
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._:/-".contains(&b));
    if valid {
        Ok(())
    } else {
        Err(Error::bad_request())
    }
}

pub fn validate_uuid(id: Uuid) -> Result<()> {
    if id.get_version() == Some(Version::Random) && id.get_variant() == Variant::RFC4122 {
        Ok(())
    } else {
        Err(Error::bad_request())
    }
}

pub fn validate_hash(hash: &str) -> Result<()> {
    if hash.len() == 64
        && hash
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        Ok(())
    } else {
        Err(Error::bad_request())
    }
}

pub fn validate_code(code: &str) -> Result<()> {
    if !code.is_empty()
        && code.len() <= 64
        && code.as_bytes()[0].is_ascii_lowercase()
        && code
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
    {
        Ok(())
    } else {
        Err(Error::bad_request())
    }
}

fn push_value(value: Value) -> Result<PushRequest> {
    let mut fields = object(value)?;
    let stream = take_string(&mut fields, "stream")?;
    let epoch = take_uuid(&mut fields, "epoch")?;
    let message = fields.remove("message");
    let cancel = fields.remove("cancel");
    let operation = match (message, cancel) {
        (Some(message), None) => PushOperation::Message(CloudEvent::from_value(message)?),
        (None, Some(cancel)) => {
            let pending = pending_value(cancel)?;
            PushOperation::Cancel {
                op: pending.op,
                hash: pending.hash,
            }
        }
        _ => return Err(Error::bad_request()),
    };
    no_unknown(fields)?;
    let request = PushRequest {
        stream,
        epoch,
        operation,
    };
    validate_push(&request)?;
    Ok(request)
}

fn pull_value(value: Value) -> Result<PullRequest> {
    let mut fields = object(value)?;
    let stream = take_string(&mut fields, "stream")?;
    let epoch = take_uuid(&mut fields, "epoch")?;
    let cursor = parse_position(&take_string(&mut fields, "cursor")?)?;
    let to = match fields.remove("to") {
        None | Some(Value::Null) => None,
        Some(Value::String(text)) => Some(parse_position(&text)?),
        _ => return Err(Error::bad_request()),
    };
    let limit = optional_limit(&mut fields)?.unwrap_or(DEFAULT_LIMIT);
    no_unknown(fields)?;
    let request = PullRequest {
        stream,
        epoch,
        cursor,
        to,
        limit,
    };
    validate_pull(&request)?;
    Ok(request)
}

fn snapshot_value(value: Value) -> Result<SnapshotRequest> {
    let mut fields = object(value)?;
    let stream = take_string(&mut fields, "stream")?;
    let epoch = take_uuid(&mut fields, "epoch")?;
    let client = take_uuid(&mut fields, "client")?;
    let attempt = take_uuid(&mut fields, "attempt")?;
    let page = take(&mut fields, "page")?;
    let limit = optional_limit(&mut fields)?;
    let kind = match page {
        Value::Null => {
            let pending = match fields.remove("pending") {
                None => Vec::new(),
                Some(Value::Array(items)) if items.len() <= 100 => items
                    .into_iter()
                    .map(pending_value)
                    .collect::<Result<Vec<_>>>()?,
                _ => return Err(Error::bad_request()),
            };
            SnapshotKind::Create {
                limit: limit.unwrap_or(DEFAULT_LIMIT),
                pending,
            }
        }
        Value::String(page) => {
            let snapshot = take_uuid(&mut fields, "snapshot")?;
            SnapshotKind::Page {
                snapshot,
                page,
                limit,
            }
        }
        _ => return Err(Error::bad_request()),
    };
    no_unknown(fields)?;
    let request = SnapshotRequest {
        stream,
        epoch,
        client,
        attempt,
        kind,
    };
    validate_snapshot(&request)?;
    Ok(request)
}

fn pending_value(value: Value) -> Result<Pending> {
    let mut fields = object(value)?;
    let op = take_uuid(&mut fields, "op")?;
    let hash = take_string(&mut fields, "hash")?;
    validate_hash(&hash)?;
    no_unknown(fields)?;
    Ok(Pending { op, hash })
}

impl Serialize for PushRequest {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(3))?;
        map.serialize_entry("stream", &self.stream)?;
        map.serialize_entry("epoch", &self.epoch)?;
        match &self.operation {
            PushOperation::Message(message) => map.serialize_entry("message", message)?,
            PushOperation::Cancel { op, hash } => {
                #[derive(Serialize)]
                struct Cancel<'a> {
                    op: &'a Uuid,
                    hash: &'a str,
                }
                map.serialize_entry("cancel", &Cancel { op, hash })?;
            }
        }
        map.end()
    }
}

impl Serialize for SnapshotRequest {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("stream", &self.stream)?;
        map.serialize_entry("epoch", &self.epoch)?;
        map.serialize_entry("client", &self.client)?;
        map.serialize_entry("attempt", &self.attempt)?;
        match &self.kind {
            SnapshotKind::Create { limit, pending } => {
                map.serialize_entry("page", &Value::Null)?;
                map.serialize_entry("limit", limit)?;
                map.serialize_entry("pending", pending)?;
            }
            SnapshotKind::Page {
                snapshot,
                page,
                limit,
            } => {
                map.serialize_entry("snapshot", snapshot)?;
                map.serialize_entry("page", page)?;
                if let Some(limit) = limit {
                    map.serialize_entry("limit", limit)?;
                }
            }
        }
        map.end()
    }
}

macro_rules! deserialize_request {
    ($type:ty, $parse:ident) => {
        impl<'de> Deserialize<'de> for $type {
            fn deserialize<D: Deserializer<'de>>(
                deserializer: D,
            ) -> std::result::Result<Self, D::Error> {
                $parse(StrictValue::deserialize(deserializer)?.0).map_err(de::Error::custom)
            }
        }
    };
}
deserialize_request!(PushRequest, push_value);
deserialize_request!(PullRequest, pull_value);
deserialize_request!(SnapshotRequest, snapshot_value);
deserialize_request!(Pending, pending_value);

fn object(value: Value) -> Result<Map<String, Value>> {
    match value {
        Value::Object(map) => Ok(map),
        _ => Err(Error::bad_request()),
    }
}
fn take(fields: &mut Map<String, Value>, key: &str) -> Result<Value> {
    fields.remove(key).ok_or_else(Error::bad_request)
}
fn take_string(fields: &mut Map<String, Value>, key: &str) -> Result<String> {
    match take(fields, key)? {
        Value::String(text) => Ok(text),
        _ => Err(Error::bad_request()),
    }
}
fn take_uuid(fields: &mut Map<String, Value>, key: &str) -> Result<Uuid> {
    parse_uuid(&take_string(fields, key)?)
}
fn parse_uuid(text: &str) -> Result<Uuid> {
    let id = Uuid::parse_str(text).map_err(|_| Error::bad_request())?;
    validate_uuid(id)?;
    if id.to_string() != text {
        return Err(Error::bad_request());
    }
    Ok(id)
}
fn no_unknown(fields: Map<String, Value>) -> Result<()> {
    if fields.is_empty() {
        Ok(())
    } else {
        Err(Error::bad_request())
    }
}
fn optional_limit(fields: &mut Map<String, Value>) -> Result<Option<usize>> {
    match fields.remove("limit") {
        None => Ok(None),
        Some(value) => {
            let limit = value
                .as_u64()
                .and_then(|v| usize::try_from(v).ok())
                .ok_or_else(Error::bad_request)?;
            validate_limit(limit)?;
            Ok(Some(limit))
        }
    }
}
fn validate_limit(limit: usize) -> Result<()> {
    if (1..=100).contains(&limit) {
        Ok(())
    } else {
        Err(Error::bad_request())
    }
}
fn required_string<'a>(fields: &'a Map<String, Value>, key: &str) -> Result<&'a str> {
    fields
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(Error::bad_request)
}
fn bounded_string<'a>(fields: &'a Map<String, Value>, key: &str, max: usize) -> Result<&'a str> {
    let text = required_string(fields, key)?;
    if text.is_empty() || text.len() > max {
        Err(Error::bad_request())
    } else {
        Ok(text)
    }
}
fn decode_data(encoded: &str) -> Result<Vec<u8>> {
    if encoded.len() > MAX_BASE64_BYTES {
        return Err(Error::new(ErrorCode::PayloadTooLarge));
    }
    let data = STANDARD.decode(encoded).map_err(|_| Error::bad_request())?;
    if data.len() > MAX_DATA_BYTES {
        return Err(Error::new(ErrorCode::PayloadTooLarge));
    }
    if STANDARD.encode(&data) != encoded {
        return Err(Error::bad_request());
    }
    Ok(data)
}
fn extension_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 20
        && name.as_bytes()[0].is_ascii_lowercase()
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
}
fn cloud_string(text: &str) -> bool {
    text.chars().all(|c| {
        let n = c as u32;
        !(n <= 0x1f
            || (0x7f..=0x9f).contains(&n)
            || (0xfdd0..=0xfdef).contains(&n)
            || n & 0xffff == 0xfffe
            || n & 0xffff == 0xffff)
    })
}

// RFC 3986 syntax, without WHATWG URL repair/normalization. The wire hash must
// retain the original URI spelling. `absolute` is CloudEvents' absolute-URI
// type (no fragment); source accepts any nonempty URI-reference.
fn uri_reference(text: &str, absolute: bool) -> bool {
    if text.is_empty() || !text.is_ascii() {
        return false;
    }
    let (without_fragment, fragment) = match text.split_once('#') {
        Some((head, fragment)) => (head, Some(fragment)),
        None => (text, None),
    };
    if absolute && fragment.is_some() {
        return false;
    }
    if fragment.is_some_and(|part| !uri_chars(part, b":@/?")) {
        return false;
    }
    let (hierarchy, query) = match without_fragment.split_once('?') {
        Some((head, query)) => (head, Some(query)),
        None => (without_fragment, None),
    };
    if query.is_some_and(|part| !uri_chars(part, b":@/?")) {
        return false;
    }
    let first = hierarchy.split('/').next().unwrap_or("");
    let (path, has_scheme) = match first.find(':') {
        Some(colon) => {
            let scheme = &first[..colon];
            if scheme.is_empty()
                || !scheme.as_bytes()[0].is_ascii_alphabetic()
                || !scheme
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"+-.".contains(&b))
            {
                return false;
            }
            (&hierarchy[colon + 1..], true)
        }
        None => (hierarchy, false),
    };
    if absolute && !has_scheme {
        return false;
    }
    if let Some(rest) = path.strip_prefix("//") {
        let (authority, path) = rest.split_once('/').map_or((rest, ""), |(a, p)| (a, p));
        authority_valid(authority) && uri_chars(path, b":@/")
    } else {
        uri_chars(path, b":@/")
    }
}

fn uri_chars(text: &str, extra: &[u8]) -> bool {
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'%' {
            if i + 2 >= bytes.len()
                || !bytes[i + 1].is_ascii_hexdigit()
                || !bytes[i + 2].is_ascii_hexdigit()
            {
                return false;
            }
            i += 3;
        } else if b.is_ascii_alphanumeric() || b"-._~!$&'()*+,;=".contains(&b) || extra.contains(&b)
        {
            i += 1;
        } else {
            return false;
        }
    }
    true
}

fn authority_valid(authority: &str) -> bool {
    let host_port = match authority.split_once('@') {
        Some((user, host)) if uri_chars(user, b":") => host,
        Some(_) => return false,
        None => authority,
    };
    if let Some(literal) = host_port.strip_prefix('[') {
        let Some((host, suffix)) = literal.split_once(']') else {
            return false;
        };
        let ip = host.parse::<Ipv6Addr>().is_ok() || ipv_future(host);
        ip && (suffix.is_empty()
            || suffix
                .strip_prefix(':')
                .is_some_and(|port| port.bytes().all(|b| b.is_ascii_digit())))
    } else {
        let (host, port) = host_port
            .split_once(':')
            .map_or((host_port, None), |(h, p)| (h, Some(p)));
        uri_chars(host, b"") && port.is_none_or(|p| p.bytes().all(|b| b.is_ascii_digit()))
    }
}

fn ipv_future(host: &str) -> bool {
    let Some(host) = host.strip_prefix('v').or_else(|| host.strip_prefix('V')) else {
        return false;
    };
    let Some((version, address)) = host.split_once('.') else {
        return false;
    };
    !version.is_empty()
        && version.bytes().all(|b| b.is_ascii_hexdigit())
        && !address.is_empty()
        && !address.contains('%')
        && uri_chars(address, b":")
}

fn timestamp(text: &str) -> bool {
    let b = text.as_bytes();
    if b.len() < 20 || !text.is_ascii() {
        return false;
    }
    for (i, byte) in b.iter().take(19).enumerate() {
        let valid = match i {
            4 | 7 => *byte == b'-',
            10 => matches!(byte, b'T' | b't'),
            13 | 16 => *byte == b':',
            _ => byte.is_ascii_digit(),
        };
        if !valid {
            return false;
        }
    }
    let mut tail = &text[19..];
    if let Some(fraction) = tail.strip_prefix('.') {
        let n = fraction.bytes().take_while(u8::is_ascii_digit).count();
        if n == 0 {
            return false;
        }
        tail = &fraction[n..];
    }
    let zone_valid = matches!(tail, "Z" | "z") || {
        let z = tail.as_bytes();
        z.len() == 6
            && matches!(z[0], b'+' | b'-')
            && z[3] == b':'
            && [1, 2, 4, 5].into_iter().all(|i| z[i].is_ascii_digit())
    };
    zone_valid && chrono::DateTime::parse_from_rfc3339(text).is_ok()
}

struct StrictValue(Value);

impl<'de> Deserialize<'de> for StrictValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        struct StrictVisitor;
        impl<'de> Visitor<'de> for StrictVisitor {
            type Value = StrictValue;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("unambiguous JSON")
            }
            fn visit_bool<E: de::Error>(self, v: bool) -> std::result::Result<Self::Value, E> {
                Ok(StrictValue(Value::Bool(v)))
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> std::result::Result<Self::Value, E> {
                Ok(StrictValue(Value::Number(v.into())))
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> std::result::Result<Self::Value, E> {
                Ok(StrictValue(Value::Number(v.into())))
            }
            fn visit_f64<E: de::Error>(self, v: f64) -> std::result::Result<Self::Value, E> {
                Number::from_f64(v)
                    .map(|n| StrictValue(Value::Number(n)))
                    .ok_or_else(|| E::custom("non-finite number"))
            }
            fn visit_str<E: de::Error>(self, v: &str) -> std::result::Result<Self::Value, E> {
                Ok(StrictValue(Value::String(v.into())))
            }
            fn visit_string<E: de::Error>(self, v: String) -> std::result::Result<Self::Value, E> {
                Ok(StrictValue(Value::String(v)))
            }
            fn visit_unit<E: de::Error>(self) -> std::result::Result<Self::Value, E> {
                Ok(StrictValue(Value::Null))
            }
            fn visit_none<E: de::Error>(self) -> std::result::Result<Self::Value, E> {
                Ok(StrictValue(Value::Null))
            }
            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut values = Vec::new();
                while let Some(value) = seq.next_element::<StrictValue>()? {
                    values.push(value.0);
                }
                Ok(StrictValue(Value::Array(values)))
            }
            fn visit_map<A: MapAccess<'de>>(
                self,
                mut map: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut values = Map::new();
                while let Some(key) = map.next_key::<String>()? {
                    if values.contains_key(&key) {
                        return Err(de::Error::custom("duplicate JSON key"));
                    }
                    values.insert(key, map.next_value::<StrictValue>()?.0);
                }
                Ok(StrictValue(Value::Object(values)))
            }
        }
        deserializer.deserialize_any(StrictVisitor)
    }
}

fn parse_json(bytes: &[u8]) -> Result<Value> {
    if bytes.len() > MAX_BODY_BYTES {
        return Err(Error::new(ErrorCode::PayloadTooLarge));
    }
    let text = std::str::from_utf8(bytes).map_err(|_| Error::bad_request())?;
    serde_json::from_str::<StrictValue>(text)
        .map(|value| value.0)
        .map_err(|_| Error::bad_request())
}

/// Decode an owner's wire response with the same body and unambiguous-JSON
/// checks as requests. Callers also check the operation's response context.
pub fn decode_response<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    let value = parse_json(bytes).map_err(|_| Error::internal())?;
    serde_json::from_value(value).map_err(|_| Error::internal())
}

/// A nullable required field: unlike a bare Option in a derived struct, this
/// adapter makes an absent key an error while still accepting explicit null.
pub fn deserialize_required_option<'de, D, T>(
    deserializer: D,
) -> std::result::Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}
