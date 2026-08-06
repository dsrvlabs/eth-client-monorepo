//! Process metrics collector (Architecture §4.4 / §12.2).
//!
//! Linux: reads `/proc/self/{stat,status,fd}` and `/proc/stat` (btime). No `unsafe`.
//! Non-Linux: no-op that warns once (containers are Linux; macOS dev loses only these series).

use prometheus_client::collector::Collector;
use prometheus_client::encoding::DescriptorEncoder;

/// Linux clock ticks per second. Virtually always 100; avoids `sysconf` under
/// workspace `unsafe_code = "deny"`.
#[cfg(target_os = "linux")]
const CLK_TCK: f64 = 100.0;

/// Collector exporting the five process series required by CC-05/3.
#[derive(Debug, Default)]
pub(crate) struct ProcessCollector;

impl Collector for ProcessCollector {
    fn encode(&self, encoder: DescriptorEncoder<'_>) -> Result<(), std::fmt::Error> {
        encode_process_metrics(encoder)
    }
}

#[cfg(target_os = "linux")]
fn encode_process_metrics(mut encoder: DescriptorEncoder<'_>) -> Result<(), std::fmt::Error> {
    use prometheus_client::encoding::EncodeMetric;
    use prometheus_client::metrics::MetricType;
    use prometheus_client::metrics::counter::ConstCounter;
    use prometheus_client::metrics::gauge::ConstGauge;
    use prometheus_client::registry::Unit;

    let Some(snap) = read_linux_snapshot() else {
        return Ok(());
    };

    let cpu = ConstCounter::new(snap.cpu_seconds);
    let e = encoder.encode_descriptor(
        "process_cpu",
        "Total user and system CPU time spent in seconds",
        Some(&Unit::Seconds),
        MetricType::Counter,
    )?;
    cpu.encode(e)?;

    let rss = ConstGauge::new(snap.resident_memory_bytes as i64);
    let e = encoder.encode_descriptor(
        "process_resident_memory",
        "Resident memory size in bytes",
        Some(&Unit::Bytes),
        MetricType::Gauge,
    )?;
    rss.encode(e)?;

    let vms = ConstGauge::new(snap.virtual_memory_bytes as i64);
    let e = encoder.encode_descriptor(
        "process_virtual_memory",
        "Virtual memory size in bytes",
        Some(&Unit::Bytes),
        MetricType::Gauge,
    )?;
    vms.encode(e)?;

    let start = ConstGauge::new(snap.start_time_seconds);
    let e = encoder.encode_descriptor(
        "process_start_time",
        "Start time of the process since unix epoch in seconds",
        Some(&Unit::Seconds),
        MetricType::Gauge,
    )?;
    start.encode(e)?;

    let fds = ConstGauge::new(snap.open_fds as i64);
    let e = encoder.encode_descriptor(
        "process_open_fds",
        "Number of open file descriptors",
        None,
        MetricType::Gauge,
    )?;
    fds.encode(e)?;

    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn encode_process_metrics(_encoder: DescriptorEncoder<'_>) -> Result<(), std::fmt::Error> {
    use std::sync::Once;
    static WARN: Once = Once::new();
    WARN.call_once(|| {
        tracing::warn!(
            "process metrics collector is a no-op on this platform (Linux /proc required)"
        );
    });
    Ok(())
}

#[cfg(target_os = "linux")]
#[derive(Debug)]
struct LinuxSnapshot {
    cpu_seconds: f64,
    resident_memory_bytes: u64,
    virtual_memory_bytes: u64,
    start_time_seconds: f64,
    open_fds: u64,
}

#[cfg(target_os = "linux")]
fn read_linux_snapshot() -> Option<LinuxSnapshot> {
    use std::fs;

    let stat = fs::read_to_string("/proc/self/stat").ok()?;
    let (utime, stime, starttime, vsize) = parse_stat(&stat)?;
    let rss_kb = parse_status_vm_rss_kb(&fs::read_to_string("/proc/self/status").ok()?)?;
    let btime = parse_btime(&fs::read_to_string("/proc/stat").ok()?)?;
    let open_fds = count_open_fds().unwrap_or(0);

    Some(LinuxSnapshot {
        cpu_seconds: (utime + stime) as f64 / CLK_TCK,
        resident_memory_bytes: rss_kb.saturating_mul(1024),
        virtual_memory_bytes: vsize,
        start_time_seconds: btime as f64 + (starttime as f64 / CLK_TCK),
        open_fds,
    })
}

/// Parse `/proc/self/stat` after the variable-length `(comm)` field.
///
/// Returns `(utime, stime, starttime, vsize)`.
#[cfg(target_os = "linux")]
fn parse_stat(stat: &str) -> Option<(u64, u64, u64, u64)> {
    let close = stat.rfind(')')?;
    // After ") " the remaining fields are space-separated starting at field 3 (state).
    let rest = stat.get(close + 2..)?;
    let fields: Vec<&str> = rest.split_whitespace().collect();
    // Relative to field 3: utime=14 → idx 11, stime=15 → 12, starttime=22 → 19, vsize=23 → 20.
    let utime: u64 = fields.get(11)?.parse().ok()?;
    let stime: u64 = fields.get(12)?.parse().ok()?;
    let starttime: u64 = fields.get(19)?.parse().ok()?;
    let vsize: u64 = fields.get(20)?.parse().ok()?;
    Some((utime, stime, starttime, vsize))
}

#[cfg(target_os = "linux")]
fn parse_status_vm_rss_kb(status: &str) -> Option<u64> {
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb = rest.split_whitespace().next()?;
            return kb.parse().ok();
        }
    }
    None
}

#[cfg(target_os = "linux")]
fn parse_btime(stat: &str) -> Option<u64> {
    for line in stat.lines() {
        if let Some(rest) = line.strip_prefix("btime ") {
            return rest.trim().parse().ok();
        }
    }
    None
}

#[cfg(target_os = "linux")]
fn count_open_fds() -> std::io::Result<u64> {
    use std::fs;

    let mut n = 0u64;
    for entry in fs::read_dir("/proc/self/fd")? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            n += 1;
        }
    }
    Ok(n)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use prometheus_client::encoding::text::encode;
    use prometheus_client::registry::Registry;

    #[test]
    fn process_collector_registers_and_encodes() {
        let mut registry = Registry::default();
        registry.register_collector(Box::new(ProcessCollector));
        let mut buf = String::new();
        encode(&mut buf, &registry).unwrap();

        #[cfg(target_os = "linux")]
        {
            assert!(
                buf.contains("process_cpu_seconds_total"),
                "missing cpu metric: {buf}"
            );
            assert!(
                buf.contains("process_resident_memory_bytes"),
                "missing rss: {buf}"
            );
            assert!(
                buf.contains("process_virtual_memory_bytes"),
                "missing vms: {buf}"
            );
            assert!(
                buf.contains("process_start_time_seconds"),
                "missing start: {buf}"
            );
            assert!(buf.contains("process_open_fds"), "missing fds: {buf}");
        }

        #[cfg(not(target_os = "linux"))]
        {
            // No-op: none of the process series should appear.
            assert!(
                !buf.contains("process_cpu_seconds_total"),
                "non-Linux must not emit process series: {buf}"
            );
            assert!(
                !buf.contains("process_open_fds"),
                "non-Linux must not emit process series: {buf}"
            );
        }
    }
}
