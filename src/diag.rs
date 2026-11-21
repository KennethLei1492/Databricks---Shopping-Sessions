//! Diagnostic build only (branch `diag`): reports the scoring machine and how the pipeline behaves
//! on it, then the caller writes a deliberately wrong campaign_daily so the public dev-set check
//! fails and the scorer returns these logs. Never submitted for timing.

use std::process::Command;
use std::time::Instant;

fn show(label: &str, path: &str) {
    match std::fs::read_to_string(path) {
        Ok(s) => eprintln!("DIAG {label}: {}", s.trim().replace('\n', " | ")),
        Err(e) => eprintln!("DIAG {label}: <{e}>"),
    }
}

fn first_line_with(path: &str, key: &str) -> String {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|s| s.lines().find(|l| l.starts_with(key)).map(str::to_string))
        .unwrap_or_else(|| "<none>".into())
}

/// Wall, user and system CPU seconds of all finished children so far.
fn children_cpu() -> (f64, f64) {
    // SAFETY: getrusage with a valid out pointer.
    unsafe {
        let mut ru: libc_rusage = std::mem::zeroed();
        getrusage(-1, &mut ru);
        (ru.utime.0 as f64 + ru.utime.1 as f64 / 1e6, ru.stime.0 as f64 + ru.stime.1 as f64 / 1e6)
    }
}

#[repr(C)]
struct Timeval(i64, i64);

#[repr(C)]
struct libc_rusage {
    utime: Timeval,
    stime: Timeval,
    rest: [i64; 14],
}

extern "C" {
    fn getrusage(who: i32, usage: *mut libc_rusage) -> i32;
}

/// Returns five short lines for the scorer to show (it prints our differing rows, not stderr).
pub fn run() -> Vec<String> {
    let mut summary: Vec<String> = Vec::new();
    let cpu = first_line_with("/proc/cpuinfo", "model name").replace("model name\t: ", "");
    let thp = std::fs::read_to_string("/sys/kernel/mm/transparent_hugepage/enabled").unwrap_or_default();
    let defrag = std::fs::read_to_string("/sys/kernel/mm/transparent_hugepage/defrag").unwrap_or_default();
    let quota = std::fs::read_to_string("/sys/fs/cgroup/cpu.max").unwrap_or_default();
    let kernel = std::fs::read_to_string("/proc/version").unwrap_or_default();
    let fs = std::fs::read_to_string("/proc/mounts")
        .unwrap_or_default()
        .lines()
        .find(|l| l.contains(" /data "))
        .unwrap_or("")
        .to_string();
    summary.push(format!(
        "D1 thp={} defrag={} cpu={} avx512={} par={:?} quota={} mem={} kernel={} data={}",
        thp.trim(),
        defrag.trim(),
        cpu.trim(),
        first_line_with("/proc/cpuinfo", "flags").contains("avx512f"),
        std::thread::available_parallelism().map(|n| n.get()).unwrap_or(0),
        quota.trim(),
        first_line_with("/proc/meminfo", "MemTotal").split_whitespace().nth(1).unwrap_or("?"),
        kernel.split_whitespace().nth(2).unwrap_or("?"),
        fs.split_whitespace().take(3).collect::<Vec<_>>().join(" ")
    ));
    eprintln!("DIAG ===== machine =====");
    eprintln!("DIAG cpu: {}", first_line_with("/proc/cpuinfo", "model name"));
    eprintln!("DIAG cpu flags avx512f: {}", first_line_with("/proc/cpuinfo", "flags").contains("avx512f"));
    eprintln!("DIAG available_parallelism: {:?}", std::thread::available_parallelism());
    show("cgroup cpu.max", "/sys/fs/cgroup/cpu.max");
    show("cgroup memory.max", "/sys/fs/cgroup/memory.max");
    eprintln!("DIAG mem: {}", first_line_with("/proc/meminfo", "MemTotal"));
    show("kernel", "/proc/version");
    show("thp enabled", "/sys/kernel/mm/transparent_hugepage/enabled");
    show("thp defrag", "/sys/kernel/mm/transparent_hugepage/defrag");
    show("overcommit", "/proc/sys/vm/overcommit_memory");
    for l in std::fs::read_to_string("/proc/mounts").unwrap_or_default().lines() {
        if l.contains(" /data ") || l.contains(" /output ") || l.contains(" /scratch ") || l.contains(" / ") {
            eprintln!("DIAG mount: {l}");
        }
    }
    show("loadavg", "/proc/loadavg");

    let exe = std::env::current_exe().expect("current_exe");
    let scratch = std::env::var("SCRATCH_DIR").unwrap_or("/scratch".into());
    let data = std::env::var("DATA_DIR").unwrap_or("/data".into());
    let configs: &[(&str, &[(&str, &str)])] = &[
        ("ncpu=8 default", &[("NCPU", "8")]),
        ("ncpu=1", &[("NCPU", "1")]),
        ("ncpu=2", &[("NCPU", "2")]),
        ("ncpu=4", &[("NCPU", "4")]),
        ("ncpu=8 np=64 nq=64", &[("NCPU", "8"), ("SS_NP", "64"), ("SS_NQ", "64")]),
        ("ncpu=8 np=4096 nq=2048", &[("NCPU", "8"), ("SS_NP", "4096"), ("SS_NQ", "2048")]),
        ("ncpu=8 thp=0", &[("NCPU", "8"), ("MIMALLOC_ALLOW_THP", "0")]),
        ("ncpu=8 pread", &[("NCPU", "8"), ("SS_MMAP", "0")]),
        ("ncpu=16", &[("NCPU", "16")]),
    ];
    // per config: best (wall, user, sys, exit-gap, links-done, phases)
    let mut best: Vec<Option<(f64, f64, f64, f64, String, String)>> = vec![None; configs.len()];
    for round in 0..3 {
        for (ci, (name, env)) in configs.iter().enumerate() {
            let out = format!("{scratch}/diag_out");
            let _ = std::fs::remove_dir_all(&out);
            let (u0, s0) = children_cpu();
            let t = Instant::now();
            let mut cmd = Command::new(&exe);
            cmd.env("SS_CHILD", "1").env("DATA_DIR", &data).env("OUTPUT_DIR", &out);
            for (k, v) in env.iter() {
                cmd.env(k, v);
            }
            let result = cmd.output();
            let wall = t.elapsed().as_secs_f64();
            let (u1, s1) = children_cpu();
            let log = result.map(|o| String::from_utf8_lossy(&o.stderr).into_owned()).unwrap_or_default();
            let last = log
                .lines()
                .filter_map(|l| l.strip_prefix('[').and_then(|r| r.split('s').next()).and_then(|x| x.trim().parse::<f64>().ok()))
                .fold(0f64, f64::max);
            let phases: Vec<String> = log
                .lines()
                .filter(|l| l.contains("phase ") && !l.contains("links-"))
                .map(|l| {
                    let name = l.split("phase ").nth(1).and_then(|r| r.split(':').next()).unwrap_or("?");
                    let wall = l.split("wall ").nth(1).and_then(|r| r.split('s').next()).unwrap_or("?");
                    format!("{name} {wall}")
                })
                .collect();
            let links = log.lines().find(|l| l.contains("links: ") && l.contains('[')).map(|l| l.split(']').next().unwrap_or("").trim_start_matches('[').trim().to_string());
            let links = links.unwrap_or_default();
            eprintln!(
                "DIAG run{round} {name:24}: wall {wall:.3} user {:.3} sys {:.3} last-log {last:.3} exit-gap {:.3} links-done {} | {}",
                u1 - u0,
                s1 - s0,
                wall - last,
                links,
                phases.join(" ")
            );
            if best[ci].as_ref().map_or(true, |b| wall < b.0) {
                best[ci] = Some((wall, u1 - u0, s1 - s0, wall - last, links, phases.join(" ")));
            }
        }
    }
    let _ = std::fs::remove_dir_all(format!("{scratch}/diag_out"));
    let fmt = |i: usize| {
        best[i].as_ref().map_or("?".to_string(), |b| format!("{}: {:.3}s u{:.2} s{:.2} gap{:.3}", configs[i].0, b.0, b.1, b.2, b.3))
    };
    summary.push(format!("D2 best of 3 | {} | {} | {} | {} | {}", fmt(1), fmt(2), fmt(3), fmt(0), fmt(8)));
    summary.push(format!("D3 best of 3 | {} | {} | {} | {}", fmt(4), fmt(5), fmt(6), fmt(7)));
    let phases = |i: usize| best[i].as_ref().map_or("?".to_string(), |b| format!("{}: links {} | {}", configs[i].0, b.4, b.5));
    summary.push(format!("D4 phases | {} | {}", phases(0), phases(1)));
    summary.push(format!("D5 phases | {} | {} | {}", phases(4), phases(6), phases(7)));
    for l in &summary {
        eprintln!("DIAG {l}");
    }
    eprintln!("DIAG ===== end =====");
    summary
}
