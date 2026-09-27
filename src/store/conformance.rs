//! Backend conformance suite: every `Store` must pass it.
//! SKELETON — the store agent writes the checks; other backends call `run_all`.

use super::Store;
use crate::error::Result;

/// Run every conformance check against stores produced by `make` (each call
/// must return a fresh, empty store).
pub fn run_all<S: Store>(make: &mut dyn FnMut() -> S) -> Result<()> {
    let _ = make;
    Ok(())
}
