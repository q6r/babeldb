//! babeldb for Node.js: a native, embedded binding (napi-rs v3).
//!
//! The store is the embedding recipe of babeldb's README ("Usar em outro
//! projeto"): fjall + write-ahead log, writes through a group committer.
//! `db` holds the `Database` class and `open()`, `js` the conversions between
//! JavaScript values and Rust data and the error codes; the chat-key helpers
//! are below.

#![deny(clippy::all)]
// napi-derive registers the exports only outside `cfg(test)`.
#![cfg_attr(test, allow(dead_code))]

mod db;
mod js;

use babeldb::scale::chat;
use napi::bindgen_prelude::Unknown;
use napi::{Env, JsValue};
use napi_derive::napi;

use crate::js::{BResult, Js, Raw};

/// The 16-byte key of chat message `id` in `channel`: `channel` (u64
/// big-endian) followed by `id` (u64 big-endian). Keys sort by channel, then
/// by id: with growing ids (snowflakes), the newest messages of a channel
/// are a reverse scan of {@link channelPrefix} with a limit.
#[napi(ts_args_type = "channel: bigint | number, id: bigint | number", ts_return_type = "Buffer")]
pub fn message_key(env: &Env, channel: Unknown<'_>, id: Unknown<'_>) -> napi::Result<Raw> {
    let js = Js::of(env);
    let key = || -> BResult<[u8; 16]> {
        let channel = js.u64_value(channel.raw(), "channel")?;
        let id = js.u64_value(id.raw(), "id")?;
        Ok(chat::message_key(channel, id))
    };
    match key() {
        Ok(key) => Ok(Raw(js.buffer(key.to_vec())?)),
        Err(e) => Err(js.error(e)),
    }
}

/// Inverse of {@link messageKey}: `[channel, id]`, or `null` when `key` is
/// not 16 bytes long.
#[napi(ts_args_type = "key: Key", ts_return_type = "[bigint, bigint] | null")]
pub fn parse_message_key(env: &Env, key: Unknown<'_>) -> napi::Result<Raw> {
    let js = Js::of(env);
    let key = js.bytes(key.raw(), "key").map_err(|e| js.error(e))?;
    let value = match chat::parse_message_key(&key) {
        Some((channel, id)) => js.array(2, [js.bigint(channel), js.bigint(id)].into_iter())?,
        None => js.null()?,
    };
    Ok(Raw(value))
}

/// The 8-byte prefix shared by every message key of `channel`: scan it with
/// `{ prefix: channelPrefix(c), reverse: true, limit: n }` for the n newest
/// messages.
#[napi(ts_args_type = "channel: bigint | number", ts_return_type = "Buffer")]
pub fn channel_prefix(env: &Env, channel: Unknown<'_>) -> napi::Result<Raw> {
    let js = Js::of(env);
    let channel = js.u64_value(channel.raw(), "channel").map_err(|e| js.error(e))?;
    Ok(Raw(js.buffer(chat::channel_prefix(channel).to_vec())?))
}
