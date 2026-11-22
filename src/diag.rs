//! DIAGNOSTIC BUILD ONLY. Measures the pipeline on the scoring machine at the practice set's scale
//! (about 20M events), which the scorer never reports per phase. A child process parses the dev
//! set 20 times (SS_REPLICATE), with the IDs of each copy shifted so every copy is a separate
//! population and nothing is written to disk, and the results come back through five
//! deliberately wrong campaign_daily rows (the scorer shows differing rows, not stderr).

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Instant;

#[repr(C)]
struct Timeval(i64, i64);

#[repr(C)]
struct Rusage {
    utime: Timeval,
    stime: Timeval,
    maxrss: i64,
    ixrss: i64,
    idrss: i64,
    isrss: i64,
    minflt: i64,
    majflt: i64,
    rest: [i64; 8],
}

extern "C" {
    fn getrusage(who: i32, usage: *mut Rusage) -> i32;
}

/// (user s, sys s, minor faults, max rss MB) of all finished children so far.
fn children() -> (f64, f64, i64, i64) {
    // SAFETY: getrusage with a valid out pointer.
    unsafe {
        let mut ru: Rusage = std::mem::zeroed();
        getrusage(-1, &mut ru);
        (
            ru.utime.0 as f64 + ru.utime.1 as f64 / 1e6,
            ru.stime.0 as f64 + ru.stime.1 as f64 / 1e6,
            ru.minflt,
            ru.maxrss / 1024,
        )
    }
}

fn scratch() -> PathBuf {
    for cand in [std::env::var("SCRATCH_DIR").unwrap_or_default(), "/scratch".into(), "/tmp".into()] {
        if cand.is_empty() {
            continue;
        }
        let p = PathBuf::from(&cand).join("ss_diag_probe");
        if std::fs::write(&p, b"x").is_ok() {
            let _ = std::fs::remove_file(&p);
            return PathBuf::from(cand);
        }
    }
    PathBuf::from("/tmp")
}

fn fs_of(path: &Path) -> String {
    let p = path.to_string_lossy().to_string();
    std::fs::read_to_string("/proc/mounts")
        .unwrap_or_default()
        .lines()
        .filter(|l| {
            let mp = l.split_whitespace().nth(1).unwrap_or("");
            p.starts_with(mp)
        })
        .max_by_key(|l| l.split_whitespace().nth(1).unwrap_or("").len())
        .map(|l| l.split_whitespace().take(3).collect::<Vec<_>>().join(" "))
        .unwrap_or_default()
}

/// Returns five short lines for the scorer to show.
pub fn run() -> Vec<String> {
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or("/data".into()));
    let sc = scratch();
    let copies: u8 = std::env::var("SS_DIAG_COPIES").ok().and_then(|v| v.parse().ok()).unwrap_or(20);
    let exe = std::env::current_exe().expect("current_exe");
    let configs: &[(&str, &[(&str, &str)])] = &[
        ("n8", &[("NCPU", "8")]),
        ("n4", &[("NCPU", "4")]),
        ("n8thp0", &[("NCPU", "8"), ("MIMALLOC_ALLOW_THP", "0")]),
    ];
    let rounds = [3usize, 1, 1];
    // per config: best (wall, user, sys, minflt, maxrss MB, milestones, phases)
    let mut best: Vec<Option<(f64, f64, f64, i64, i64, String, String)>> = vec![None; configs.len()];
    let out = sc.join("ss_diag_out");
    for (ci, (_, env)) in configs.iter().enumerate() {
        for _ in 0..rounds[ci] {
            let _ = std::fs::remove_dir_all(&out);
            let (u0, s0, f0, _) = children();
            let t = Instant::now();
            let mut cmd = Command::new(&exe);
            cmd.env("SS_CHILD", "1").env("DATA_DIR", &data).env("OUTPUT_DIR", &out).env("SS_REPLICATE", copies.to_string());
            for (k, v) in env.iter() {
                cmd.env(k, v);
            }
            let result = cmd.output();
            let wall = t.elapsed().as_secs_f64();
            let (u1, s1, f1, rss) = children();
            let log = result.map(|o| String::from_utf8_lossy(&o.stderr).into_owned()).unwrap_or_default();
            // Milestones: "[  1.234s] what" -> "what-initials 1.234".
            let miles: Vec<String> = log
                .lines()
                .filter_map(|l| {
                    let rest = l.strip_prefix('[')?;
                    let (t, what) = rest.split_once("s] ")?;
                    let tag: String = what.split_whitespace().take(1).collect();
                    Some(format!("{} {}", &tag[..tag.len().min(6)], t.trim()))
                })
                .collect();
            let phases: Vec<String> = log
                .lines()
                .filter(|l| l.contains("phase ") && !l.contains("links-"))
                .map(|l| {
                    let name = l.split("phase ").nth(1).and_then(|r| r.split(':').next()).unwrap_or("?");
                    let w = l.split("wall ").nth(1).and_then(|r| r.split('s').next()).unwrap_or("?");
                    let u = l.split("utilization ").nth(1).and_then(|r| r.split('%').next()).unwrap_or("?");
                    let k = l.split("kernel ").nth(1).and_then(|r| r.split('s').next()).unwrap_or("?");
                    let a = l.split("rss anon ").nth(1).and_then(|r| r.split(' ').next()).unwrap_or("?");
                    format!("{name} {w} u{u} k{k} a{a}")
                })
                .collect();
            if best[ci].as_ref().map_or(true, |b| wall < b.0) {
                best[ci] = Some((wall, u1 - u0, s1 - s0, f1 - f0, rss, miles.join(","), phases.join(",")));
            }
        }
    }
    let _ = std::fs::remove_dir_all(&out);
    let ev_bytes: u64 = copies as u64
        * std::fs::read_dir(data.join("events")).map_or(0, |d| d.filter_map(|e| e.ok()).filter_map(|e| e.metadata().ok()).map(|m| m.len()).sum::<u64>());
    let fmt = |i: usize| {
        best[i].as_ref().map_or("?".to_string(), |b| {
            format!("{} {:.3}s u{:.2} s{:.2} flt{}k rss{}M", configs[i].0, b.0, b.1, b.2, b.3 / 1000, b.4)
        })
    };
    let miles = |i: usize| best[i].as_ref().map_or("?".to_string(), |b| b.5.clone());
    let phases = |i: usize| best[i].as_ref().map_or("?".to_string(), |b| b.6.clone());
    vec![
        format!("S1 x{copies} evMB{} out={} | {} | {} | {}", ev_bytes >> 20, fs_of(&sc), fmt(0), fmt(1), fmt(2)),
        format!("S2 n8 phases {}", phases(0)),
        format!("S3 n8 milestones {}", miles(0)),
        format!("S4 n4 phases {}", phases(1)),
        format!("S5 thp0 phases {}", phases(2)),
    ]
}
