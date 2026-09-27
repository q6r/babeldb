//! OS measurements: file allocation, process resource usage, memory and
//! volume information, and an optional process memory limit.
//!
//! Windows uses Win32 calls (the only `unsafe` code of this module, each call
//! documented). Linux reads `/proc`. Other platforms report what `std` can
//! measure and leave the rest at 0 / `None`.

use std::path::{Path, PathBuf};

use crate::stats::FileSize;

/// Apparent and allocated size of a file.
///
/// `allocated_bytes` is what the file system reserved for the file:
/// Windows `FILE_STANDARD_INFO.AllocationSize` (cluster-rounded; NTFS keeps
/// very small files resident in their MFT record and then reports an
/// allocation smaller than one cluster), Unix `st_blocks * 512`. It is `None`
/// for directories and where it cannot be measured.
pub fn file_size(path: &Path) -> std::io::Result<FileSize> {
    let meta = std::fs::metadata(path)?;
    let allocated_bytes = if meta.is_file() {
        allocated_bytes(path, &meta)
    } else {
        None
    };
    Ok(FileSize {
        path: path.to_path_buf(),
        apparent_bytes: meta.len(),
        allocated_bytes,
    })
}

#[cfg(windows)]
fn allocated_bytes(path: &Path, _meta: &std::fs::Metadata) -> Option<u64> {
    win::allocation_size(path).ok()
}

#[cfg(unix)]
fn allocated_bytes(_path: &Path, meta: &std::fs::Metadata) -> Option<u64> {
    use std::os::unix::fs::MetadataExt;
    Some(meta.blocks() * 512)
}

#[cfg(not(any(windows, unix)))]
fn allocated_bytes(_path: &Path, _meta: &std::fs::Metadata) -> Option<u64> {
    None
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
///
/// Windows: `GetProcessMemoryInfo` (working set, peak working set,
/// `PrivateUsage` = commit charge, page faults = soft + hard),
/// `GetProcessTimes` (CPU times, updated at clock-tick granularity, usually
/// 15.6 ms), `GetProcessIoCounters` (every ReadFile/WriteFile-class call of
/// the process, including those served by the file cache; memory-mapped
/// reads are not counted).
///
/// Linux: `/proc/self/status` (VmRSS, VmHWM, RssAnon as private bytes),
/// `/proc/self/stat` (minor + major faults, utime/stime assuming
/// USER_HZ = 100), `/proc/self/io` (rchar, wchar, syscr, syscw).
pub fn process_metrics() -> ProcessMetrics {
    platform_process_metrics()
}

#[cfg(windows)]
fn platform_process_metrics() -> ProcessMetrics {
    win::process_metrics()
}

#[cfg(target_os = "linux")]
fn platform_process_metrics() -> ProcessMetrics {
    let mut m = ProcessMetrics::default();
    if let Ok(text) = std::fs::read_to_string("/proc/self/status") {
        let s = parse_proc_status(&text);
        m.working_set_bytes = s.rss;
        m.peak_working_set_bytes = s.hwm;
        m.private_bytes = s.anon;
    }
    if let Some((faults, utime, stime)) = std::fs::read_to_string("/proc/self/stat")
        .ok()
        .and_then(|t| parse_proc_stat(&t))
    {
        m.page_faults = faults;
        m.user_cpu_ms = utime as f64 * 10.0;
        m.kernel_cpu_ms = stime as f64 * 10.0;
    }
    if let Ok(text) = std::fs::read_to_string("/proc/self/io") {
        let io = parse_proc_io(&text);
        m.io_read_bytes = io.rchar;
        m.io_write_bytes = io.wchar;
        m.io_read_ops = io.syscr;
        m.io_write_ops = io.syscw;
    }
    m
}

#[cfg(not(any(windows, target_os = "linux")))]
fn platform_process_metrics() -> ProcessMetrics {
    ProcessMetrics::default()
}

/// Physical memory of the machine.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MemoryStatus {
    pub total_bytes: u64,
    /// Memory available without paging (Windows `ullAvailPhys`: free + standby
    /// list; Linux `MemAvailable`). It includes the file cache, so it says how
    /// much of the OS cache is reclaimable, not how much is empty.
    pub available_bytes: u64,
}

/// Physical memory totals (`None` where unsupported).
pub fn memory_status() -> Option<MemoryStatus> {
    #[cfg(windows)]
    {
        win::memory_status()
    }
    #[cfg(target_os = "linux")]
    {
        std::fs::read_to_string("/proc/meminfo")
            .ok()
            .and_then(|t| parse_meminfo(&t))
    }
    #[cfg(not(any(windows, target_os = "linux")))]
    {
        None
    }
}

/// File system hosting a path.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct VolumeInfo {
    /// Mount point / volume root (e.g. `C:\`).
    pub root: PathBuf,
    /// File system name (e.g. `NTFS`).
    pub file_system: String,
    /// Allocation unit (cluster) size.
    pub cluster_bytes: u64,
    pub total_bytes: u64,
    /// Free bytes available to the caller.
    pub free_bytes: u64,
}

/// Volume information of the file system containing `path` (Windows only;
/// `Unsupported` elsewhere).
pub fn volume_info(path: &Path) -> std::io::Result<VolumeInfo> {
    #[cfg(windows)]
    {
        win::volume_info(path)
    }
    #[cfg(not(windows))]
    {
        let _ = path;
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "volume_info is implemented for Windows only",
        ))
    }
}

/// Cap the commit charge (private memory) of the current process at `bytes`
/// with a Windows job object (`JOB_OBJECT_LIMIT_PROCESS_MEMORY`), the RAM
/// budget mechanism of spec §11. Allocations beyond the budget fail, which
/// aborts a Rust process. It does NOT limit the OS file cache nor the pages of
/// memory-mapped files (LMDB), which are not commit-charged. The limit lasts
/// for the lifetime of the process. On Linux use a cgroup instead
/// (`Unsupported` here).
pub fn limit_process_memory(bytes: u64) -> std::io::Result<()> {
    #[cfg(windows)]
    {
        win::limit_process_memory(bytes)
    }
    #[cfg(not(windows))]
    {
        let _ = bytes;
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "use a cgroup to limit memory on this platform",
        ))
    }
}

// ---------------------------------------------------------------------------
// /proc parsers (pure; compiled everywhere so they are unit-tested on every
// platform, used at run time on Linux only)
// ---------------------------------------------------------------------------

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct ProcStatus {
    rss: u64,
    hwm: u64,
    anon: u64,
}

/// `VmRSS`, `VmHWM` and `RssAnon` of `/proc/self/status`, in bytes.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn parse_proc_status(text: &str) -> ProcStatus {
    let mut s = ProcStatus::default();
    for line in text.lines() {
        let mut it = line.split_whitespace();
        let (Some(name), Some(kib)) = (it.next(), it.next().and_then(|v| v.parse::<u64>().ok()))
        else {
            continue;
        };
        match name {
            "VmRSS:" => s.rss = kib * 1024,
            "VmHWM:" => s.hwm = kib * 1024,
            "RssAnon:" => s.anon = kib * 1024,
            _ => {}
        }
    }
    s
}

/// (minor + major page faults, utime ticks, stime ticks) of `/proc/self/stat`.
/// The command name (field 2) may contain spaces and parentheses, so fields
/// are counted after the last `)`.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn parse_proc_stat(text: &str) -> Option<(u64, u64, u64)> {
    let rest = &text[text.rfind(')')? + 1..];
    let fields: Vec<&str> = rest.split_whitespace().collect();
    // fields[0] is field 3 (state); minflt = 10, majflt = 12, utime = 14, stime = 15.
    let field = |n: usize| -> Option<u64> { fields.get(n - 3)?.parse().ok() };
    Some((field(10)? + field(12)?, field(14)?, field(15)?))
}

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct ProcIo {
    rchar: u64,
    wchar: u64,
    syscr: u64,
    syscw: u64,
}

/// `rchar`, `wchar`, `syscr`, `syscw` of `/proc/self/io`.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn parse_proc_io(text: &str) -> ProcIo {
    let mut io = ProcIo::default();
    for line in text.lines() {
        let Some((name, v)) = line.split_once(':') else {
            continue;
        };
        let Ok(v) = v.trim().parse::<u64>() else {
            continue;
        };
        match name.trim() {
            "rchar" => io.rchar = v,
            "wchar" => io.wchar = v,
            "syscr" => io.syscr = v,
            "syscw" => io.syscw = v,
            _ => {}
        }
    }
    io
}

/// `MemTotal` and `MemAvailable` of `/proc/meminfo`.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn parse_meminfo(text: &str) -> Option<MemoryStatus> {
    let mut total = None;
    let mut available = None;
    for line in text.lines() {
        let mut it = line.split_whitespace();
        let (Some(name), Some(kib)) = (it.next(), it.next().and_then(|v| v.parse::<u64>().ok()))
        else {
            continue;
        };
        match name {
            "MemTotal:" => total = Some(kib * 1024),
            "MemAvailable:" => available = Some(kib * 1024),
            _ => {}
        }
    }
    Some(MemoryStatus {
        total_bytes: total?,
        available_bytes: available.unwrap_or(0),
    })
}

// ---------------------------------------------------------------------------
// Windows
// ---------------------------------------------------------------------------

#[cfg(windows)]
mod win {
    use std::ffi::c_void;
    use std::io;
    use std::mem::size_of;
    use std::os::windows::ffi::{OsStrExt, OsStringExt};
    use std::os::windows::fs::OpenOptionsExt;
    use std::os::windows::io::AsRawHandle;
    use std::path::{Path, PathBuf};

    use windows_sys::Win32::Foundation::{CloseHandle, FILETIME, HANDLE};
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_READ_ATTRIBUTES, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
        FILE_STANDARD_INFO, FileStandardInfo, GetDiskFreeSpaceExW, GetDiskFreeSpaceW,
        GetFileInformationByHandleEx, GetVolumeInformationW, GetVolumePathNameW,
    };
    use windows_sys::Win32::System::JobObjects::{
        AssignProcessToJobObject, JOB_OBJECT_LIMIT_PROCESS_MEMORY,
        JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation,
        SetInformationJobObject,
    };
    use windows_sys::Win32::System::ProcessStatus::{
        GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS, PROCESS_MEMORY_COUNTERS_EX,
    };
    use windows_sys::Win32::System::Threading::{
        GetCurrentProcess, GetProcessIoCounters, GetProcessTimes, IO_COUNTERS,
    };

    use super::{MemoryStatus, ProcessMetrics, VolumeInfo};

    /// Declarations for two kernel32 functions whose windows-sys bindings sit
    /// behind features this crate does not enable (`Win32_Security` for the
    /// `SECURITY_ATTRIBUTES` pointer type, `Win32_System_SystemInformation`).
    /// Signatures and the `MEMORYSTATUSEX` layout follow the Windows SDK.
    mod ffi {
        use std::ffi::c_void;

        use windows_sys::Win32::Foundation::HANDLE;

        #[repr(C)]
        #[derive(Default)]
        pub struct MemoryStatusEx {
            pub dw_length: u32,
            pub dw_memory_load: u32,
            pub ull_total_phys: u64,
            pub ull_avail_phys: u64,
            pub ull_total_page_file: u64,
            pub ull_avail_page_file: u64,
            pub ull_total_virtual: u64,
            pub ull_avail_virtual: u64,
            pub ull_avail_extended_virtual: u64,
        }

        #[link(name = "kernel32")]
        unsafe extern "system" {
            pub fn GlobalMemoryStatusEx(buffer: *mut MemoryStatusEx) -> i32;
            pub fn CreateJobObjectW(job_attributes: *const c_void, name: *const u16) -> HANDLE;
        }
    }

    fn filetime_100ns(t: &FILETIME) -> u64 {
        (u64::from(t.dwHighDateTime) << 32) | u64::from(t.dwLowDateTime)
    }

    fn wide_nul(path: &Path) -> Vec<u16> {
        path.as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect()
    }

    fn until_nul(buf: &[u16]) -> &[u16] {
        &buf[..buf.iter().position(|&c| c == 0).unwrap_or(buf.len())]
    }

    pub fn allocation_size(path: &Path) -> io::Result<u64> {
        // Attribute-only access: never conflicts with the share mode of the
        // database handles that keep the file open.
        let file = std::fs::OpenOptions::new()
            .access_mode(FILE_READ_ATTRIBUTES)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
            .open(path)?;
        let mut info = FILE_STANDARD_INFO::default();
        // SAFETY: `file` owns a valid handle for the duration of the call;
        // `info` is a writable FILE_STANDARD_INFO and the buffer size passed is
        // exactly its size, as the FileStandardInfo class requires.
        let ok = unsafe {
            GetFileInformationByHandleEx(
                file.as_raw_handle() as HANDLE,
                FileStandardInfo,
                (&mut info as *mut FILE_STANDARD_INFO).cast::<c_void>(),
                size_of::<FILE_STANDARD_INFO>() as u32,
            )
        };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(u64::try_from(info.AllocationSize).unwrap_or(0))
    }

    pub fn process_metrics() -> ProcessMetrics {
        let mut m = ProcessMetrics::default();
        // SAFETY: GetCurrentProcess has no preconditions; it returns a
        // pseudo-handle that is always valid and must not be closed.
        let process = unsafe { GetCurrentProcess() };

        let mut mem = PROCESS_MEMORY_COUNTERS_EX {
            cb: size_of::<PROCESS_MEMORY_COUNTERS_EX>() as u32,
            ..Default::default()
        };
        // SAFETY: `mem` is a writable PROCESS_MEMORY_COUNTERS_EX whose `cb`
        // holds its size; the API accepts the extended layout through the base
        // pointer type when `cb` says so.
        let ok = unsafe {
            GetProcessMemoryInfo(
                process,
                (&mut mem as *mut PROCESS_MEMORY_COUNTERS_EX).cast::<PROCESS_MEMORY_COUNTERS>(),
                mem.cb,
            )
        };
        if ok != 0 {
            m.working_set_bytes = mem.WorkingSetSize as u64;
            m.peak_working_set_bytes = mem.PeakWorkingSetSize as u64;
            m.private_bytes = mem.PrivateUsage as u64;
            m.page_faults = u64::from(mem.PageFaultCount);
        }

        let (mut creation, mut exit, mut kernel, mut user) = (
            FILETIME::default(),
            FILETIME::default(),
            FILETIME::default(),
            FILETIME::default(),
        );
        // SAFETY: valid pseudo-handle and four writable FILETIME out-parameters.
        let ok =
            unsafe { GetProcessTimes(process, &mut creation, &mut exit, &mut kernel, &mut user) };
        if ok != 0 {
            m.kernel_cpu_ms = filetime_100ns(&kernel) as f64 / 10_000.0;
            m.user_cpu_ms = filetime_100ns(&user) as f64 / 10_000.0;
        }

        let mut io = IO_COUNTERS::default();
        // SAFETY: valid pseudo-handle and a writable IO_COUNTERS out-parameter.
        let ok = unsafe { GetProcessIoCounters(process, &mut io) };
        if ok != 0 {
            m.io_read_bytes = io.ReadTransferCount;
            m.io_write_bytes = io.WriteTransferCount;
            m.io_read_ops = io.ReadOperationCount;
            m.io_write_ops = io.WriteOperationCount;
        }
        m
    }

    pub fn memory_status() -> Option<MemoryStatus> {
        let mut s = ffi::MemoryStatusEx {
            dw_length: size_of::<ffi::MemoryStatusEx>() as u32,
            ..Default::default()
        };
        // SAFETY: `s` is a writable MEMORYSTATUSEX (same layout as the SDK
        // struct) whose dwLength holds its size, as the API requires.
        let ok = unsafe { ffi::GlobalMemoryStatusEx(&mut s) };
        (ok != 0).then_some(MemoryStatus {
            total_bytes: s.ull_total_phys,
            available_bytes: s.ull_avail_phys,
        })
    }

    pub fn volume_info(path: &Path) -> io::Result<VolumeInfo> {
        let path = std::path::absolute(path)?;
        let wide_path = wide_nul(&path);
        let mut root = vec![0u16; 1024];
        // SAFETY: `wide_path` is NUL-terminated; `root` is writable for the
        // number of UTF-16 units passed.
        let ok =
            unsafe { GetVolumePathNameW(wide_path.as_ptr(), root.as_mut_ptr(), root.len() as u32) };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        let root_len = until_nul(&root).len();
        root.truncate(root_len + 1); // keep the terminator for the next calls

        let mut fs_name = [0u16; 64];
        // SAFETY: `root` is a NUL-terminated volume root; the optional
        // out-parameters are null (allowed) and `fs_name` is writable for the
        // length passed.
        let ok = unsafe {
            GetVolumeInformationW(
                root.as_ptr(),
                std::ptr::null_mut(),
                0,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                fs_name.as_mut_ptr(),
                fs_name.len() as u32,
            )
        };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }

        let (mut sectors_per_cluster, mut bytes_per_sector, mut free_clusters, mut total_clusters) =
            (0u32, 0u32, 0u32, 0u32);
        // SAFETY: NUL-terminated root and four writable u32 out-parameters.
        let ok = unsafe {
            GetDiskFreeSpaceW(
                root.as_ptr(),
                &mut sectors_per_cluster,
                &mut bytes_per_sector,
                &mut free_clusters,
                &mut total_clusters,
            )
        };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }

        let (mut available, mut total, mut free) = (0u64, 0u64, 0u64);
        // SAFETY: NUL-terminated root and three writable u64 out-parameters.
        let ok =
            unsafe { GetDiskFreeSpaceExW(root.as_ptr(), &mut available, &mut total, &mut free) };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(VolumeInfo {
            root: PathBuf::from(std::ffi::OsString::from_wide(until_nul(&root))),
            file_system: String::from_utf16_lossy(until_nul(&fs_name)),
            cluster_bytes: u64::from(sectors_per_cluster) * u64::from(bytes_per_sector),
            total_bytes: total,
            free_bytes: available,
        })
    }

    pub fn limit_process_memory(bytes: u64) -> io::Result<()> {
        let limit = usize::try_from(bytes).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "memory limit exceeds usize")
        })?;
        // SAFETY: null security attributes (default security) and a null name
        // (unnamed job) are documented as valid arguments.
        let job = unsafe { ffi::CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
        if job.is_null() {
            return Err(io::Error::last_os_error());
        }
        let mut info = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
        info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_PROCESS_MEMORY;
        info.ProcessMemoryLimit = limit;
        // SAFETY: `job` is the job handle created above; `info` is a fully
        // initialised JOBOBJECT_EXTENDED_LIMIT_INFORMATION and the length
        // passed is its size, as the JobObjectExtendedLimitInformation class
        // requires.
        let mut ok = unsafe {
            SetInformationJobObject(
                job,
                JobObjectExtendedLimitInformation,
                (&info as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast::<c_void>(),
                size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
            )
        };
        if ok != 0 {
            // SAFETY: `job` is valid; GetCurrentProcess returns the current
            // process pseudo-handle (nested jobs are supported since Windows 8).
            ok = unsafe { AssignProcessToJobObject(job, GetCurrentProcess()) };
        }
        let result = if ok != 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        };
        // The process keeps the job (and its limit) alive once assigned; the
        // job has no KILL_ON_JOB_CLOSE flag, so closing our handle is safe.
        // SAFETY: `job` is a handle we own and close exactly once.
        unsafe { CloseHandle(job) };
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use std::time::{Duration, Instant};

    #[test]
    fn file_size_apparent_and_allocated() {
        let dir = tempfile::tempdir().unwrap();
        let big = dir.path().join("big.bin");
        std::fs::write(&big, vec![7u8; 10_000]).unwrap();
        let s = file_size(&big).unwrap();
        assert_eq!(s.apparent_bytes, 10_000);
        assert_eq!(s.path, big);
        if cfg!(any(windows, unix)) {
            let alloc = s
                .allocated_bytes
                .expect("allocated size is measured on this platform");
            // Rounded up to whole clusters/blocks, never below the data size.
            assert!((10_000..10_000 + (1 << 20)).contains(&alloc), "{alloc}");
            assert_eq!(alloc % 512, 0, "{alloc}");
        }

        // Tiny file: NTFS may keep it resident in the MFT record (allocation
        // below one cluster); it never exceeds a few clusters anywhere.
        let tiny = dir.path().join("tiny.bin");
        std::fs::write(&tiny, b"abc").unwrap();
        let s = file_size(&tiny).unwrap();
        assert_eq!(s.apparent_bytes, 3);
        if let Some(a) = s.allocated_bytes {
            assert!(a <= 64 * 1024, "{a}");
        }

        // Directories report no allocation; missing files are errors.
        assert_eq!(file_size(dir.path()).unwrap().allocated_bytes, None);
        assert!(file_size(&dir.path().join("missing")).is_err());
    }

    #[cfg(windows)]
    #[test]
    fn allocated_size_while_the_file_is_open_for_writing() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("open.bin");
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&p)
            .unwrap();
        f.write_all(&vec![1u8; 70_000]).unwrap();
        f.sync_all().unwrap();
        let s = file_size(&p).unwrap();
        assert_eq!(s.apparent_bytes, 70_000);
        assert!(s.allocated_bytes.unwrap() >= 70_000);
    }

    #[test]
    fn process_metrics_are_measured() {
        // Do some I/O and allocation so every counter moves.
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("io.bin");
        std::fs::write(&p, vec![3u8; 256 * 1024]).unwrap();
        let mut buf = Vec::new();
        std::fs::File::open(&p)
            .unwrap()
            .read_to_end(&mut buf)
            .unwrap();
        let hog: Vec<u64> = (0..2_000_000u64)
            .map(|x| x.wrapping_mul(0x9E37_79B9))
            .collect();
        assert_eq!(hog.len(), 2_000_000);

        let m = process_metrics();
        if cfg!(any(windows, target_os = "linux")) {
            assert!(m.working_set_bytes > 0, "{m:?}");
            assert!(m.peak_working_set_bytes >= m.working_set_bytes, "{m:?}");
            assert!(m.private_bytes > 0, "{m:?}");
            assert!(m.page_faults > 0, "{m:?}");
            // CPU time advances in clock ticks (~15.6 ms on Windows): spin until it moves.
            let start = Instant::now();
            let mut x = 0u64;
            while process_metrics().user_cpu_ms <= m.user_cpu_ms
                && start.elapsed() < Duration::from_secs(20)
            {
                for i in 0..200_000u64 {
                    x = x.wrapping_add(std::hint::black_box(i));
                }
            }
            std::hint::black_box(x);
            assert!(
                process_metrics().user_cpu_ms > m.user_cpu_ms,
                "user CPU time never advanced"
            );
            assert!(m.io_read_bytes >= 256 * 1024, "{m:?}");
            assert!(m.io_write_bytes >= 256 * 1024, "{m:?}");
            assert!(m.io_read_ops > 0 && m.io_write_ops > 0, "{m:?}");
        }
        // Counters are cumulative.
        let later = process_metrics();
        assert!(later.io_read_bytes >= m.io_read_bytes);
        assert!(later.page_faults >= m.page_faults);
        assert!(later.user_cpu_ms >= m.user_cpu_ms);
    }

    #[cfg(windows)]
    #[test]
    fn windows_memory_and_volume() {
        let mem = memory_status().expect("GlobalMemoryStatusEx");
        assert!(mem.total_bytes > 1 << 30, "{mem:?}");
        assert!(
            mem.available_bytes > 0 && mem.available_bytes <= mem.total_bytes,
            "{mem:?}"
        );

        let dir = tempfile::tempdir().unwrap();
        let v = volume_info(dir.path()).unwrap();
        assert!(!v.file_system.is_empty(), "{v:?}");
        assert!(
            v.cluster_bytes >= 512 && v.cluster_bytes.is_power_of_two(),
            "{v:?}"
        );
        assert!(v.total_bytes > 0 && v.free_bytes <= v.total_bytes, "{v:?}");
        assert!(v.root.to_string_lossy().ends_with('\\'), "{v:?}");
    }

    #[cfg(windows)]
    #[test]
    fn memory_limit_can_be_applied() {
        // A 1 TiB budget: exercises the job-object path without constraining
        // the test process.
        limit_process_memory(1 << 40).unwrap();
        let v: Vec<u8> = vec![1; 1 << 20];
        assert_eq!(v.len(), 1 << 20);
    }

    #[test]
    fn proc_parsers() {
        let status = "Name:\tbench\nVmPeak:\t  900 kB\nVmHWM:\t    2048 kB\nVmRSS:\t    1024 kB\nRssAnon:\t     512 kB\n";
        assert_eq!(
            parse_proc_status(status),
            ProcStatus {
                rss: 1024 * 1024,
                hwm: 2048 * 1024,
                anon: 512 * 1024
            }
        );

        // Field 2 contains spaces and a parenthesis.
        let stat = "4242 (my (odd) cmd) S 1 4242 4242 0 -1 4194560 150 0 7 0 250 40 0 0 20 0 1 0 100 1000 200";
        assert_eq!(parse_proc_stat(stat), Some((157, 250, 40)));
        assert_eq!(parse_proc_stat("garbage"), None);

        let io = "rchar: 100\nwchar: 200\nsyscr: 3\nsyscw: 4\nread_bytes: 4096\nwrite_bytes: 0\n";
        assert_eq!(
            parse_proc_io(io),
            ProcIo {
                rchar: 100,
                wchar: 200,
                syscr: 3,
                syscw: 4
            }
        );

        let meminfo =
            "MemTotal:       32000000 kB\nMemFree:  100 kB\nMemAvailable:   16000000 kB\n";
        assert_eq!(
            parse_meminfo(meminfo),
            Some(MemoryStatus {
                total_bytes: 32_000_000 * 1024,
                available_bytes: 16_000_000 * 1024
            })
        );
        assert_eq!(parse_meminfo("nothing"), None);
    }
}
