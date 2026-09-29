//! Conversions between JavaScript values and Rust data (raw Node-API calls),
//! and the errors the binding throws and rejects with: JavaScript `Error`s
//! carrying a stable `code`.

use std::ffi::{CString, c_char};
use std::ptr;

use napi::bindgen_prelude::{Buffer, ToNapiValue, TypeName, Unknown};
use napi::{Env, Status, ValueType, sys};

/// Stable `code` of every error the binding raises. The binding's own
/// conditions (`CONFLICT`, `CLOSED`, `ALREADY_OPEN`, `INVALID_ARGUMENT`) come
/// first; the other codes are named after the `babeldb::Error` variant.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Code {
    /// A write's expectation (`ifAbsent` / `ifRevision`) did not hold.
    Conflict,
    /// The handle was closed.
    Closed,
    /// The directory is already open (in this process or in another one).
    AlreadyOpen,
    /// A malformed argument, or one the engine refuses (empty key, key or
    /// value above the limits, ...).
    InvalidArgument,
    Io,
    Backend,
    Format,
    Integrity,
    UnknownCodec,
    UnknownGenerator,
    MissingDependency,
    IdExhausted,
    Unsupported,
    /// A variant added to `babeldb::Error` after this binding.
    Unknown,
    /// A bug or an unexpected state in the binding.
    Internal,
}

impl Code {
    pub fn as_str(self) -> &'static str {
        match self {
            Code::Conflict => "CONFLICT",
            Code::Closed => "CLOSED",
            Code::AlreadyOpen => "ALREADY_OPEN",
            Code::InvalidArgument => "INVALID_ARGUMENT",
            Code::Io => "IO",
            Code::Backend => "BACKEND",
            Code::Format => "FORMAT",
            Code::Integrity => "INTEGRITY",
            Code::UnknownCodec => "UNKNOWN_CODEC",
            Code::UnknownGenerator => "UNKNOWN_GENERATOR",
            Code::MissingDependency => "MISSING_DEPENDENCY",
            Code::IdExhausted => "ID_EXHAUSTED",
            Code::Unsupported => "UNSUPPORTED",
            Code::Unknown => "UNKNOWN",
            Code::Internal => "INTERNAL",
        }
    }
}

/// An error on its way to JavaScript. Built on any thread; turned into a JS
/// `Error` on the JavaScript thread ([`Js::error`]).
#[derive(Debug)]
pub struct BindError {
    pub code: Code,
    pub message: String,
}

pub type BResult<T> = std::result::Result<T, BindError>;

impl BindError {
    pub fn new(code: Code, message: impl Into<String>) -> BindError {
        BindError {
            code,
            message: message.into(),
        }
    }

    pub fn invalid(message: impl Into<String>) -> BindError {
        BindError::new(Code::InvalidArgument, message)
    }

    pub fn closed() -> BindError {
        BindError::new(Code::Closed, "the database is closed")
    }

    pub fn internal(message: impl Into<String>) -> BindError {
        BindError::new(Code::Internal, message)
    }
}

/// `RevisionConflict` -> `CONFLICT`; `InvalidArgument` and `LimitExceeded` ->
/// `INVALID_ARGUMENT` (as in the Python binding); every other variant -> its
/// own name (`IO`, `BACKEND`, `INTEGRITY`, ...).
impl From<babeldb::Error> for BindError {
    fn from(e: babeldb::Error) -> BindError {
        use babeldb::Error as E;
        let code = match &e {
            E::RevisionConflict { .. } => Code::Conflict,
            E::InvalidArgument(_) | E::LimitExceeded(_) => Code::InvalidArgument,
            E::Io(_) => Code::Io,
            E::Backend(_) => Code::Backend,
            E::Format(_) => Code::Format,
            E::Integrity { .. } => Code::Integrity,
            E::UnknownCodec { .. } => Code::UnknownCodec,
            E::UnknownGenerator { .. } => Code::UnknownGenerator,
            E::MissingDependency { .. } => Code::MissingDependency,
            E::IdExhausted(_) => Code::IdExhausted,
            E::Unsupported(_) => Code::Unsupported,
            _ => Code::Unknown,
        };
        BindError::new(code, e.to_string())
    }
}

impl From<std::io::Error> for BindError {
    fn from(e: std::io::Error) -> BindError {
        BindError::new(Code::Io, format!("I/O error: {e}"))
    }
}

/// A JavaScript value built on the JavaScript thread, handed to Node-API as
/// is (the return value of methods whose shape depends on the call).
pub struct Raw(pub sys::napi_value);

impl TypeName for Raw {
    fn type_name() -> &'static str {
        "unknown"
    }

    fn value_type() -> ValueType {
        ValueType::Unknown
    }
}

impl ToNapiValue for Raw {
    unsafe fn to_napi_value(_env: sys::napi_env, val: Self) -> napi::Result<sys::napi_value> {
        Ok(val.0)
    }
}

/// Buffers up to this size are copied into the JS heap; larger ones are
/// handed over without a copy (external memory, freed by the GC).
const COPY_MAX: usize = 32 << 10;

/// Node-API calls on the JavaScript thread. Only built from the `napi_env`
/// of a call or callback in progress (not `Send`: holds a raw pointer).
///
/// Every `unsafe` block in its methods relies on that: `self.0` is the env of
/// the call in progress on this thread, and every `napi_value` passed belongs
/// to the handle scope of that call.
#[derive(Clone, Copy)]
pub struct Js(sys::napi_env);

impl Js {
    pub fn of(env: &Env) -> Js {
        Js(env.raw())
    }

    pub fn env(self) -> Env {
        Env::from_raw(self.0)
    }

    /// Identity of the environment (main thread or worker).
    pub fn addr(self) -> usize {
        self.0 as usize
    }

    /// `process.on('exit', listener)` (emitted on a natural exit and on
    /// `process.exit()`), plus `teardown` as a cleanup hook of this
    /// environment (run when it is torn down, e.g. a terminated worker),
    /// called with the environment's address.
    pub fn install_exit_hooks(
        self,
        listener: unsafe extern "C" fn(sys::napi_env, sys::napi_callback_info) -> sys::napi_value,
        teardown: unsafe extern "C" fn(*mut std::ffi::c_void),
    ) -> napi::Result<()> {
        let result = self.call_process_on_exit(listener);
        let mut pending = false;
        unsafe {
            if sys::napi_is_exception_pending(self.0, &mut pending) == sys::Status::napi_ok && pending {
                let mut ignored = ptr::null_mut();
                sys::napi_get_and_clear_last_exception(self.0, &mut ignored);
            }
        }
        result?;
        Self::made(
            unsafe { sys::napi_add_env_cleanup_hook(self.0, Some(teardown), self.0.cast()) },
            "napi_add_env_cleanup_hook",
        )
    }

    fn call_process_on_exit(
        self,
        listener: unsafe extern "C" fn(sys::napi_env, sys::napi_callback_info) -> sys::napi_value,
    ) -> napi::Result<()> {
        let mut global = ptr::null_mut();
        Self::made(unsafe { sys::napi_get_global(self.0, &mut global) }, "napi_get_global")?;
        let mut process = ptr::null_mut();
        Self::made(
            unsafe { sys::napi_get_named_property(self.0, global, c"process".as_ptr(), &mut process) },
            "global.process",
        )?;
        let mut on = ptr::null_mut();
        Self::made(unsafe { sys::napi_get_named_property(self.0, process, c"on".as_ptr(), &mut on) }, "process.on")?;
        let mut function = ptr::null_mut();
        Self::made(
            unsafe {
                sys::napi_create_function(
                    self.0,
                    c"babeldbCloseOnExit".as_ptr(),
                    -1,
                    Some(listener),
                    ptr::null_mut(),
                    &mut function,
                )
            },
            "napi_create_function",
        )?;
        let args = [self.string_value("exit")?, function];
        let mut ignored = ptr::null_mut();
        Self::made(
            unsafe { sys::napi_call_function(self.0, process, on, args.len(), args.as_ptr(), &mut ignored) },
            "process.on('exit')",
        )
    }

    // -- inputs --------------------------------------------------------------

    /// A failed call while reading an argument: clear the exception it may
    /// have left (a throwing getter) and report the argument as invalid.
    fn read(self, status: sys::napi_status, what: &str) -> BResult<()> {
        if status == sys::Status::napi_ok {
            return Ok(());
        }
        let mut pending = false;
        unsafe {
            if sys::napi_is_exception_pending(self.0, &mut pending) == sys::Status::napi_ok && pending {
                let mut ignored = ptr::null_mut();
                sys::napi_get_and_clear_last_exception(self.0, &mut ignored);
            }
        }
        Err(BindError::invalid(format!("could not read {what} (Node-API status {status})")))
    }

    fn type_of(self, v: sys::napi_value, what: &str) -> BResult<i32> {
        let mut t = 0;
        self.read(unsafe { sys::napi_typeof(self.0, v, &mut t) }, what)?;
        Ok(t)
    }

    pub fn expect_object(self, v: sys::napi_value, what: &str) -> BResult<()> {
        if self.type_of(v, what)? == sys::ValueType::napi_object {
            Ok(())
        } else {
            Err(BindError::invalid(format!("{what} must be an object")))
        }
    }

    /// A key or value: a `Buffer`, a `Uint8Array` or a string (its UTF-8
    /// bytes), copied (the caller may reuse its buffer at once).
    pub fn bytes(self, v: sys::napi_value, what: &str) -> BResult<Vec<u8>> {
        let t = self.type_of(v, what)?;
        if t == sys::ValueType::napi_string {
            return self.utf8(v, what);
        }
        if t == sys::ValueType::napi_object {
            let mut typed = false;
            self.read(unsafe { sys::napi_is_typedarray(self.0, v, &mut typed) }, what)?;
            if typed {
                let mut kind = 0;
                let mut len = 0usize;
                let mut data = ptr::null_mut();
                let mut arraybuffer = ptr::null_mut();
                let mut offset = 0usize;
                self.read(
                    unsafe {
                        sys::napi_get_typedarray_info(
                            self.0,
                            v,
                            &mut kind,
                            &mut len,
                            &mut data,
                            &mut arraybuffer,
                            &mut offset,
                        )
                    },
                    what,
                )?;
                if kind == sys::TypedarrayType::uint8_array {
                    if len == 0 || data.is_null() {
                        return Ok(Vec::new());
                    }
                    // `data` already points at the first element (byte offset applied).
                    return Ok(unsafe { std::slice::from_raw_parts(data.cast::<u8>(), len) }.to_vec());
                }
            }
        }
        Err(BindError::invalid(format!("{what} must be a Buffer, a Uint8Array or a string")))
    }

    fn utf8(self, v: sys::napi_value, what: &str) -> BResult<Vec<u8>> {
        let mut len = 0usize;
        self.read(
            unsafe { sys::napi_get_value_string_utf8(self.0, v, ptr::null_mut(), 0, &mut len) },
            what,
        )?;
        let mut buf = vec![0u8; len + 1];
        let mut written = 0usize;
        self.read(
            unsafe {
                sys::napi_get_value_string_utf8(self.0, v, buf.as_mut_ptr().cast::<c_char>(), len + 1, &mut written)
            },
            what,
        )?;
        buf.truncate(written);
        Ok(buf)
    }

    pub fn string(self, v: sys::napi_value, what: &str) -> BResult<String> {
        if self.type_of(v, what)? != sys::ValueType::napi_string {
            return Err(BindError::invalid(format!("{what} must be a string")));
        }
        // Node-API writes valid UTF-8 (lone surrogates become U+FFFD).
        String::from_utf8(self.utf8(v, what)?).map_err(|_| BindError::invalid(format!("{what} is not valid UTF-8")))
    }

    /// A non-negative integer given as a `bigint` (up to 2^64 - 1) or as a
    /// `number` (a safe integer).
    pub fn u64_value(self, v: sys::napi_value, what: &str) -> BResult<u64> {
        let t = self.type_of(v, what)?;
        if t == sys::ValueType::napi_bigint {
            let mut x = 0u64;
            let mut lossless = false;
            self.read(unsafe { sys::napi_get_value_bigint_uint64(self.0, v, &mut x, &mut lossless) }, what)?;
            if !lossless {
                return Err(BindError::invalid(format!("{what} must be between 0 and 2^64 - 1")));
            }
            return Ok(x);
        }
        if t == sys::ValueType::napi_number {
            let mut f = 0f64;
            self.read(unsafe { sys::napi_get_value_double(self.0, v, &mut f) }, what)?;
            const MAX_SAFE: f64 = 9_007_199_254_740_991.0;
            if !(f.is_finite() && f >= 0.0 && f.fract() == 0.0 && f <= MAX_SAFE) {
                return Err(BindError::invalid(format!(
                    "{what} must be a non-negative integer (use a bigint above 2^53 - 1)"
                )));
            }
            return Ok(f as u64);
        }
        Err(BindError::invalid(format!("{what} must be a bigint or a number")))
    }

    /// Property `name` of object `obj`; `None` when it is `undefined`.
    pub fn prop(self, obj: sys::napi_value, name: &str) -> BResult<Option<sys::napi_value>> {
        let cname = CString::new(name).map_err(|_| BindError::internal("property name with a NUL byte"))?;
        let mut v = ptr::null_mut();
        let what = format!("property `{name}`");
        self.read(unsafe { sys::napi_get_named_property(self.0, obj, cname.as_ptr(), &mut v) }, &what)?;
        Ok((self.type_of(v, &what)? != sys::ValueType::napi_undefined).then_some(v))
    }

    pub fn opt_bool(self, obj: sys::napi_value, name: &str) -> BResult<Option<bool>> {
        let Some(v) = self.prop(obj, name)? else {
            return Ok(None);
        };
        if self.type_of(v, name)? != sys::ValueType::napi_boolean {
            return Err(BindError::invalid(format!("`{name}` must be a boolean")));
        }
        let mut b = false;
        self.read(unsafe { sys::napi_get_value_bool(self.0, v, &mut b) }, name)?;
        Ok(Some(b))
    }

    pub fn opt_u64(self, obj: sys::napi_value, name: &str) -> BResult<Option<u64>> {
        match self.prop(obj, name)? {
            Some(v) => Ok(Some(self.u64_value(v, &format!("`{name}`"))?)),
            None => Ok(None),
        }
    }

    pub fn opt_bytes(self, obj: sys::napi_value, name: &str) -> BResult<Option<Vec<u8>>> {
        match self.prop(obj, name)? {
            Some(v) => Ok(Some(self.bytes(v, &format!("`{name}`"))?)),
            None => Ok(None),
        }
    }

    /// Length of array `v` (an error if `v` is not an array).
    pub fn array_len(self, v: sys::napi_value, what: &str) -> BResult<u32> {
        let mut is_array = false;
        self.read(unsafe { sys::napi_is_array(self.0, v, &mut is_array) }, what)?;
        if !is_array {
            return Err(BindError::invalid(format!("{what} must be an array")));
        }
        let mut len = 0u32;
        self.read(unsafe { sys::napi_get_array_length(self.0, v, &mut len) }, what)?;
        Ok(len)
    }

    pub fn element(self, array: sys::napi_value, index: u32, what: &str) -> BResult<sys::napi_value> {
        let mut v = ptr::null_mut();
        self.read(unsafe { sys::napi_get_element(self.0, array, index, &mut v) }, what)?;
        Ok(v)
    }

    // -- outputs -------------------------------------------------------------

    fn made(status: sys::napi_status, what: &str) -> napi::Result<()> {
        if status == sys::Status::napi_ok {
            Ok(())
        } else {
            Err(napi::Error::new(Status::from(status), format!("Node-API call failed: {what}")))
        }
    }

    pub fn buffer(self, data: Vec<u8>) -> napi::Result<sys::napi_value> {
        if data.len() > COPY_MAX {
            // Handed over without a copy (napi-rs copies only where external
            // buffers are not allowed).
            return unsafe { Buffer::to_napi_value(self.0, Buffer::from(data)) };
        }
        let mut out = ptr::null_mut();
        let mut copy = ptr::null_mut();
        Self::made(
            unsafe { sys::napi_create_buffer_copy(self.0, data.len(), data.as_ptr().cast(), &mut copy, &mut out) },
            "napi_create_buffer_copy",
        )?;
        Ok(out)
    }

    pub fn bigint(self, v: u64) -> napi::Result<sys::napi_value> {
        let mut out = ptr::null_mut();
        Self::made(unsafe { sys::napi_create_bigint_uint64(self.0, v, &mut out) }, "napi_create_bigint_uint64")?;
        Ok(out)
    }

    pub fn boolean(self, b: bool) -> napi::Result<sys::napi_value> {
        let mut out = ptr::null_mut();
        Self::made(unsafe { sys::napi_get_boolean(self.0, b, &mut out) }, "napi_get_boolean")?;
        Ok(out)
    }

    pub fn null(self) -> napi::Result<sys::napi_value> {
        let mut out = ptr::null_mut();
        Self::made(unsafe { sys::napi_get_null(self.0, &mut out) }, "napi_get_null")?;
        Ok(out)
    }

    pub fn undefined(self) -> napi::Result<sys::napi_value> {
        let mut out = ptr::null_mut();
        Self::made(unsafe { sys::napi_get_undefined(self.0, &mut out) }, "napi_get_undefined")?;
        Ok(out)
    }

    /// An array of `len` elements produced by `items`.
    pub fn array(
        self,
        len: usize,
        items: impl Iterator<Item = napi::Result<sys::napi_value>>,
    ) -> napi::Result<sys::napi_value> {
        let mut out = ptr::null_mut();
        Self::made(
            unsafe { sys::napi_create_array_with_length(self.0, len, &mut out) },
            "napi_create_array_with_length",
        )?;
        for (i, item) in items.enumerate() {
            let index = u32::try_from(i).map_err(|_| napi::Error::from_reason("array too long for JavaScript"))?;
            Self::made(unsafe { sys::napi_set_element(self.0, out, index, item?) }, "napi_set_element")?;
        }
        Ok(out)
    }

    fn string_value(self, s: &str) -> napi::Result<sys::napi_value> {
        let mut out = ptr::null_mut();
        Self::made(
            unsafe { sys::napi_create_string_utf8(self.0, s.as_ptr().cast::<c_char>(), s.len() as isize, &mut out) },
            "napi_create_string_utf8",
        )?;
        Ok(out)
    }

    fn error_value(self, e: &BindError) -> napi::Result<sys::napi_value> {
        let code = self.string_value(e.code.as_str())?;
        let message = self.string_value(&e.message)?;
        let mut out = ptr::null_mut();
        // `napi_create_error` sets the `code` property.
        Self::made(unsafe { sys::napi_create_error(self.0, code, message, &mut out) }, "napi_create_error")?;
        Ok(out)
    }

    /// The JavaScript `Error` for `e` (`message`, `code`), wrapped in a
    /// `napi::Error` that references it: throwing or rejecting with it hands
    /// JavaScript this exact object.
    pub fn error(self, e: BindError) -> napi::Error {
        match self.error_value(&e) {
            Ok(v) => napi::Error::from(unsafe { Unknown::from_raw_unchecked(self.0, v) }),
            Err(_) => napi::Error::new(Status::GenericFailure, format!("{}: {}", e.code.as_str(), e.message)),
        }
    }
}
