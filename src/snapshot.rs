//! Immutable, bounded snapshot pages. The owner supplies a consistent cut and
//! receipts from an authorized publication transaction, and stores the result.
use crate::{Error, ErrorCode, Result, backend::Context, engine::*, wire::*};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;
use uuid::Uuid;

/// Persist this secret at the data owner for as long as its epochs remain valid.
/// It authenticates a page's entire context even after the staged image is lost.
#[derive(Clone)]
pub struct TokenKey([u8; 32]);
impl TokenKey {
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
    pub fn random() -> Self {
        let mut bytes = [0; 32];
        bytes[..16].copy_from_slice(Uuid::new_v4().as_bytes());
        bytes[16..].copy_from_slice(Uuid::new_v4().as_bytes());
        Self(bytes)
    }
    fn mac(
        &self,
        context: &Context,
        request: &SnapshotRequest,
        id: Uuid,
        index: usize,
    ) -> Result<Hmac<Sha256>> {
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.0).map_err(|_| Error::internal())?;
        mac.update(b"replicaman-page/1\0");
        let binding = serde_json::to_vec(&(
            &context.subject,
            &request.stream,
            request.epoch,
            request.client,
            request.attempt,
            id,
            index,
        ))
        .map_err(|_| Error::internal())?;
        mac.update(&binding);
        Ok(mac)
    }
    fn sign(
        &self,
        context: &Context,
        request: &SnapshotRequest,
        id: Uuid,
        index: usize,
    ) -> Result<String> {
        let index_bytes = u32::try_from(index)
            .map_err(|_| Error::internal())?
            .to_be_bytes();
        let tag = self
            .mac(context, request, id, index)?
            .finalize()
            .into_bytes();
        let mut bytes = Vec::with_capacity(36);
        bytes.extend_from_slice(&index_bytes);
        bytes.extend_from_slice(&tag);
        Ok(format!("1.{}", URL_SAFE_NO_PAD.encode(bytes)))
    }
    /// Check AFTER access/epoch authorization, BEFORE a snapshot cache lookup.
    /// An authenticated token whose image is missing yields snapshot_expired at
    /// the owner; a foreign/tampered token yields bad_request here.
    pub fn verify(&self, context: &Context, request: &SnapshotRequest) -> Result<Option<usize>> {
        context.check()?;
        validate_snapshot(request)?;
        let SnapshotKind::Page { snapshot, page, .. } = &request.kind else {
            return Ok(None);
        };
        let encoded = page.strip_prefix("1.").ok_or_else(Error::bad_request)?;
        let bytes = URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| Error::bad_request())?;
        if bytes.len() != 36 || URL_SAFE_NO_PAD.encode(&bytes) != encoded {
            return Err(Error::bad_request());
        }
        let index =
            u32::from_be_bytes(bytes[..4].try_into().map_err(|_| Error::bad_request())?) as usize;
        self.mac(context, request, *snapshot, index)?
            .verify_slice(&bytes[4..])
            .map_err(|_| Error::bad_request())?;
        Ok(Some(index))
    }
}

#[derive(Clone, Copy, Debug)]
pub struct SnapshotLimits {
    pub max_items: usize,
    pub max_data_bytes: usize,
    pub max_item_bytes: usize,
}
impl Default for SnapshotLimits {
    fn default() -> Self {
        Self {
            max_items: 2048,
            max_data_bytes: 64 * 1024 * 1024,
            max_item_bytes: 512 * 1024,
        }
    }
}

/// Epoch, position, time and items must come from ONE consistent state. The cut
/// time is measured on the owner's trusted clock, not supplied by the client.
pub struct SnapshotCut {
    pub epoch: Uuid,
    pub cursor: Position,
    pub at_ms: u64,
    pub items: Vec<CloudEvent>,
}

#[derive(Clone)]
pub struct Snapshot {
    subject: String,
    request: SnapshotRequest,
    id: Uuid,
    cursor: Position,
    cut_ms: u64,
    expires_ms: u64,
    last_ms: u64,
    limit: usize,
    pending: Vec<Pending>,
    pages: Vec<Vec<CloudEvent>>,
    tokens: Vec<String>,
    ops: Vec<SnapshotOp>,
}
impl Snapshot {
    /// Call in the authorized publication transaction; a previous active attempt
    /// must be looked up before quota admission or construction of another image.
    pub fn new(
        context: &Context,
        request: &SnapshotRequest,
        cut: SnapshotCut,
        ops: Vec<SnapshotOp>,
        now_ms: u64,
        limits: SnapshotLimits,
        key: &TokenKey,
    ) -> Result<Self> {
        context.check()?;
        validate_snapshot(request)?;
        let SnapshotKind::Create { limit, pending } = &request.kind else {
            return Err(Error::bad_request());
        };
        if limits.max_items == 0
            || limits.max_items > u32::MAX as usize
            || limits.max_item_bytes == 0
            || limits.max_item_bytes > MAX_DATA_BYTES
            || limits.max_data_bytes == 0
            || cut.epoch != request.epoch
            || cut.cursor.0 > i64::MAX as u64
            || cut.at_ms > now_ms
        {
            return Err(Error::internal());
        }
        if cut.items.len() > limits.max_items {
            return Err(Error::new(ErrorCode::StoragePressure));
        }
        if ops.len() != pending.len() {
            return Err(Error::internal());
        }
        for (op, original) in ops.iter().zip(pending) {
            if op.op != original.op || op.hash != original.hash {
                return Err(Error::internal());
            }
            match &op.coverage {
                Coverage::Accepted { cursor, result } => {
                    if cursor.0 == 0 || cursor.0 > i64::MAX as u64 {
                        return Err(Error::internal());
                    }
                    if let Accepted::Noop { reason } = result {
                        validate_code(reason).map_err(|_| Error::internal())?;
                    }
                }
                Coverage::Rejected { reason } => {
                    validate_code(reason).map_err(|_| Error::internal())?
                }
                Coverage::Unseen | Coverage::Mismatch => (),
            }
        }
        let hard_expiry = cut
            .at_ms
            .checked_add(SNAPSHOT_LIFETIME_MS)
            .ok_or_else(Error::internal)?;
        let expires_ms = now_ms.saturating_add(SNAPSHOT_IDLE_MS).min(hard_expiry);
        if expires_ms.saturating_sub(now_ms) < 1000 {
            return Err(Error::new(ErrorCode::SnapshotExpired));
        }
        let mut result = Self {
            subject: context.subject.clone(),
            request: request.clone(),
            id: Uuid::new_v4(),
            cursor: cut.cursor,
            cut_ms: cut.at_ms,
            expires_ms,
            last_ms: now_ms,
            limit: *limit,
            pending: pending.clone(),
            pages: vec![],
            tokens: vec![],
            ops,
        };
        let mut total = 0usize;
        let mut current = Vec::new();
        let mut current_bytes = 0usize;
        // Every signed token has the same width. Count the envelope once instead
        // of repeatedly cloning/serializing a growing page of application data.
        let token = key.sign(context, request, result.id, 1)?;
        let envelope_bytes = json_size(&result.response(vec![], Some(token), 600))?;
        for item in cut.items {
            context.check()?;
            item.validate().map_err(|_| Error::internal())?;
            let bytes = item.data()?.len();
            total = total.checked_add(bytes).ok_or_else(Error::internal)?;
            if bytes > limits.max_item_bytes || total > limits.max_data_bytes {
                return Err(Error::new(ErrorCode::StoragePressure));
            }
            let item = item.with_id(Uuid::new_v4())?;
            let bytes = json_size(&item)?;
            if !current.is_empty()
                && (current.len() == result.limit
                    || envelope_bytes + current_bytes + 1 + bytes > PAGE_BYTES)
            {
                result.pages.push(std::mem::take(&mut current));
                current_bytes = 0;
            }
            if envelope_bytes + bytes > MAX_BODY_BYTES {
                return Err(Error::internal());
            }
            current_bytes += bytes + usize::from(!current.is_empty());
            current.push(item);
        }
        if !current.is_empty() || result.pages.is_empty() {
            result.pages.push(current);
        }
        for index in 0..result.pages.len() {
            result
                .tokens
                .push(key.sign(context, request, result.id, index)?);
        }
        context.check()?;
        Ok(result)
    }
    pub fn same_slot(&self, context: &Context, request: &SnapshotRequest) -> bool {
        self.subject == context.subject
            && self.request.stream == request.stream
            && self.request.epoch == request.epoch
            && self.request.client == request.client
    }
    pub fn same_attempt(&self, context: &Context, request: &SnapshotRequest) -> bool {
        self.same_slot(context, request) && self.request.attempt == request.attempt
    }
    pub fn expired(&self, now_ms: u64) -> bool {
        self.expires_ms.saturating_sub(now_ms) < 1000
    }
    pub fn retry_after(&self, now_ms: u64) -> u64 {
        self.expires_ms.saturating_sub(now_ms).div_ceil(1000).max(1)
    }
    /// Call TokenKey::verify before lookup, then stage this update and commit
    /// only if publication/access/epoch checks and
    /// the owner's deadline still pass. Do not renew an expired or abandoned
    /// attempt merely because a different attempt is requesting its slot.
    pub fn page(
        &mut self,
        context: &Context,
        request: &SnapshotRequest,
        now_ms: u64,
    ) -> Result<SnapshotResponse> {
        context.check()?;
        validate_snapshot(request)?;
        if !self.same_attempt(context, request) {
            return Err(Error::bad_request());
        }
        if now_ms < self.last_ms {
            return Err(Error::internal());
        }
        if self.expired(now_ms) {
            return Err(Error::new(ErrorCode::SnapshotExpired));
        }
        let index = match &request.kind {
            SnapshotKind::Create { limit, pending } => {
                if *limit != self.limit || *pending != self.pending {
                    return Err(Error::new(ErrorCode::AttemptChanged));
                }
                0
            }
            SnapshotKind::Page {
                snapshot,
                page,
                limit,
            } => {
                // The attempt may have been recreated after its old image was
                // lost/expired. An authenticated old token remains valid, but
                // must never return a page from that new image.
                if *snapshot != self.id {
                    return Err(Error::new(ErrorCode::SnapshotExpired));
                }
                if limit.is_some_and(|limit| limit != self.limit) {
                    return Err(Error::new(ErrorCode::AttemptChanged));
                }
                self.tokens
                    .iter()
                    .position(|token| token == page)
                    .ok_or_else(Error::bad_request)?
            }
        };
        let expiry = now_ms
            .saturating_add(SNAPSHOT_IDLE_MS)
            .min(self.cut_ms.saturating_add(SNAPSHOT_LIFETIME_MS));
        let ttl = expiry.saturating_sub(now_ms) / 1000;
        if ttl == 0 {
            return Err(Error::new(ErrorCode::SnapshotExpired));
        }
        let response = self.response(
            self.pages[index].clone(),
            self.tokens.get(index + 1).cloned(),
            ttl as u16,
        );
        context.check()?;
        self.expires_ms = expiry;
        self.last_ms = now_ms;
        Ok(response)
    }
    fn response(&self, items: Vec<CloudEvent>, next: Option<String>, ttl: u16) -> SnapshotResponse {
        SnapshotResponse {
            stream: self.request.stream.clone(),
            epoch: self.request.epoch,
            client: self.request.client,
            attempt: self.request.attempt,
            snapshot: self.id,
            cursor: self.cursor,
            items,
            next,
            ttl,
            ops: self.ops.clone(),
        }
    }
}
