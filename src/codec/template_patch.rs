//! `TemplatePatchV1`: output rebuilt from an identified immutable template
//! (param) plus exact edit operations (copies from the template and literal
//! inserts). Decoding is copy-only; no reserialization, bytes are preserved.
//! SKELETON — to be implemented by the codec agent.

use crate::error::Result;

/// Prepared template (bytes + match index), built once per param id (Send + Sync).
pub struct Template {
    pub id: u64,
    bytes: Vec<u8>,
}

impl Template {
    pub fn new(id: u64, bytes: Vec<u8>) -> Template {
        Template { id, bytes }
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

/// Patch body of `data` against `template`, or None if not applicable.
pub fn encode(data: &[u8], template: &Template) -> Option<Vec<u8>> {
    let _ = (data, template);
    None
}

pub fn decode(body: &[u8], raw_len: u32, template: &Template, out: &mut Vec<u8>) -> Result<()> {
    let _ = (body, raw_len, template, out);
    todo!("template_patch::decode")
}

/// Choose a template from samples (e.g. the sample minimizing total patch size).
pub fn train(samples: &[Vec<u8>]) -> Option<Vec<u8>> {
    let _ = samples;
    None
}
