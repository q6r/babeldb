//! OS measurements: file allocation and process resource usage.
//! SKELETON — the bench agent implements the Windows/Unix versions.

use std::path::Path;

use crate::stats::FileSize;

/// Apparent and allocated size of a file.
pub fn file_size(path: &Path) -> std::io::Result<FileSize> {
    let meta = std::fs::metadata(path)?;
    Ok(FileSize { path: path.to_path_buf(), apparent_bytes: meta.len(), allocated_bytes: None })
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ProcessMetrics {
    pub working_set_bytes: u64,
    pub peak_working_set_bytes: u64,
    pub private_bytes: u64,
    pub page_faults: u64,
    pub user_cpu_ms: f64,
    pub kernel_cpu_ms: f64,
    /// Bytes requested from the OS by read/write calls (not physical disk I/O).
    pub io_read_bytes: u64,
    pub io_write_bytes: u64,
    pub io_read_ops: u64,
    pub io_write_ops: u64,
}

/// Current process metrics; fields that cannot be measured stay 0.
pub fn process_metrics() -> ProcessMetrics {
    ProcessMetrics::default()
}
