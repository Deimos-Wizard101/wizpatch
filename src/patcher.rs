//! Stateless parallel patcher. Streams downloads and CRC checks; one shared
//! ureq agent gives connection reuse across all jobs.
//!
//! Two decoupled stages:
//!   * CRC stage (sized to CPU count) reads local files, computes CRC, and
//!     decides whether a download is needed.
//!   * Download stage is gated by an [`AdaptiveLimit`] whose permit count is
//!     tuned by a 1Hz controller running plain AIMD on observed bytes/sec.
//! CRC for record N+1 can run while record N is downloading.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use smol::lock::{Semaphore, SemaphoreGuardArc};
use smol::unblock;

use crate::enums::{Country, Game, Platform};
use crate::errors::WizPatchError;
use crate::notifier::{get_file_list_records, FileRecord};
use crate::utils::{fix_src_path, revision_from_url};
use crate::webdriver::{build_agent, download_to_file, get_patch_urls, PatchUrls};

#[derive(Debug, Clone)]
pub struct PatchOptions {
    pub game: Game,
    pub platform: Platform,
    pub country: Country,
    pub revision: Option<String>,
    pub download_missing: bool,
    pub game_path: PathBuf,
    /// Upper bound on concurrent downloads. The controller adapts within
    /// `[1, jobs]`; it does not exceed this.
    pub jobs: usize,
    /// If true, print per-file completions and controller stats. Otherwise
    /// emit a single dd-style progress line on stderr.
    pub verbose: bool,
}

#[derive(Debug, Default, Clone)]
pub struct PatchStats {
    pub total: usize,
    pub downloaded: usize,
    pub skipped_missing: usize,
    pub up_to_date: usize,
    pub failed: usize,
}

pub async fn patch(opts: &PatchOptions) -> Result<PatchStats, WizPatchError> {
    let urls = resolve_urls(opts).await?;
    let revision = revision_from_url(&urls.file_list_url)?;
    println!("Patching revision: {revision}");
    println!("Base URL: {}", urls.base_url);

    let records = get_file_list_records(&urls.file_list_url).await?;
    let total = records.len();

    let crc_workers = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    let max_jobs = opts.jobs.max(1);
    let initial_jobs = 2.min(max_jobs);

    println!(
        "File list has {total} records. CRC workers: {crc_workers}. \
         Download permits: start {initial_jobs}, max {max_jobs}."
    );

    let agent = Arc::new(build_agent());
    let base_url = Arc::new(urls.base_url.clone());
    let game_path = Arc::new(opts.game_path.clone());

    let downloaded = Arc::new(AtomicUsize::new(0));
    let up_to_date = Arc::new(AtomicUsize::new(0));
    let skipped_missing = Arc::new(AtomicUsize::new(0));
    let failed = Arc::new(AtomicUsize::new(0));
    let progress = Arc::new(AtomicUsize::new(0));

    let bytes_total = Arc::new(AtomicU64::new(0));
    let errors_window = Arc::new(AtomicUsize::new(0));
    let verbose = opts.verbose;

    let dl_limit = Arc::new(AdaptiveLimit::new(initial_jobs, max_jobs));

    let download_missing = opts.download_missing;

    let (rec_tx, rec_rx) = async_channel::bounded::<FileRecord>(crc_workers * 2);
    let (dl_tx, dl_rx) = async_channel::bounded::<DownloadJob>(max_jobs * 2);

    // CRC stage.
    let mut crc_handles = Vec::with_capacity(crc_workers);
    for _ in 0..crc_workers {
        let rec_rx = rec_rx.clone();
        let dl_tx = dl_tx.clone();
        let game_path = game_path.clone();
        let base_url = base_url.clone();
        let up_to_date = up_to_date.clone();
        let skipped_missing = skipped_missing.clone();
        let progress = progress.clone();
        crc_handles.push(smol::spawn(async move {
            while let Ok(rec) = rec_rx.recv().await {
                match classify(&game_path, &rec, download_missing).await {
                    Decision::Download(path) => {
                        let url = format!("{}/{}", base_url, rec.src_file_name);
                        let _ = dl_tx
                            .send(DownloadJob {
                                name: rec.src_file_name,
                                url,
                                dest: path,
                            })
                            .await;
                    }
                    Decision::UpToDate => {
                        up_to_date.fetch_add(1, Ordering::Relaxed);
                        progress.fetch_add(1, Ordering::Relaxed);
                    }
                    Decision::SkippedMissing => {
                        skipped_missing.fetch_add(1, Ordering::Relaxed);
                        progress.fetch_add(1, Ordering::Relaxed);
                    }
                    Decision::SkippedPatchClient => {
                        progress.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }));
    }
    drop(rec_rx);
    drop(dl_tx);

    // Download stage. One task per permit slot, gated by the adaptive limit.
    let mut dl_handles = Vec::with_capacity(max_jobs);
    for _ in 0..max_jobs {
        let dl_rx = dl_rx.clone();
        let agent = agent.clone();
        let dl_limit = dl_limit.clone();
        let downloaded = downloaded.clone();
        let failed = failed.clone();
        let progress = progress.clone();
        let bytes_total = bytes_total.clone();
        let errors_window = errors_window.clone();
        dl_handles.push(smol::spawn(async move {
            while let Ok(job) = dl_rx.recv().await {
                let _permit = dl_limit.acquire().await;
                let result = download_to_file(
                    &agent,
                    &job.url,
                    job.dest,
                    Some(bytes_total.clone()),
                )
                .await;
                let n = progress.fetch_add(1, Ordering::Relaxed) + 1;
                match result {
                    Ok(bytes) => {
                        downloaded.fetch_add(1, Ordering::Relaxed);
                        if verbose {
                            println!("[{n:>5}/{total:>5}] {} ({} bytes)", job.name, bytes);
                        }
                    }
                    Err(e) => {
                        failed.fetch_add(1, Ordering::Relaxed);
                        errors_window.fetch_add(1, Ordering::Relaxed);
                        // Clear any in-progress renderer line, then report.
                        eprintln!("\r\x1b[2K[{n:>5}/{total:>5}] {} FAILED: {e}", job.name);
                    }
                }
            }
        }));
    }
    drop(dl_rx);

    // Throughput controller. AIMD on bytes/sec; errors trigger multiplicative
    // decrease, regression triggers additive decrease, improvement grows by 1.
    let stop_signal = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let controller = {
        let stop = stop_signal.clone();
        let dl_limit = dl_limit.clone();
        let bytes_total = bytes_total.clone();
        let errors_window = errors_window.clone();
        smol::spawn(async move {
            let mut prev_window: u64 = 0;
            let mut prev_total: u64 = 0;
            while !stop.load(Ordering::Relaxed) {
                smol::Timer::after(Duration::from_millis(1000)).await;
                let cur_total = bytes_total.load(Ordering::Relaxed);
                let bytes = cur_total.saturating_sub(prev_total);
                prev_total = cur_total;
                let errors = errors_window.swap(0, Ordering::Relaxed);
                if errors > 0 {
                    dl_limit.shrink_half();
                } else if bytes == 0 {
                    // Idle — nothing to learn from this window. Don't punish.
                } else if bytes > prev_window {
                    dl_limit.grow(1);
                } else if bytes * 100 < prev_window * 90 {
                    dl_limit.shrink(1);
                }
                prev_window = bytes;
                if verbose && (bytes > 0 || errors > 0) {
                    eprintln!(
                        "[ctl] {} KB/s, errs={}, permits={}",
                        bytes / 1024,
                        errors,
                        dl_limit.target()
                    );
                }
            }
        })
    };

    // Renderer. Non-verbose only: one updating dd-style line on stderr.
    let renderer = if !verbose {
        let stop = stop_signal.clone();
        let bytes_total = bytes_total.clone();
        let progress = progress.clone();
        let dl_limit = dl_limit.clone();
        Some(smol::spawn(async move {
            use std::io::Write;
            let mut prev_bytes: u64 = 0;
            let mut prev_t = std::time::Instant::now();
            while !stop.load(Ordering::Relaxed) {
                smol::Timer::after(Duration::from_millis(250)).await;
                let bytes = bytes_total.load(Ordering::Relaxed);
                let now = std::time::Instant::now();
                let dt = now.duration_since(prev_t).as_secs_f64();
                let mbs = if dt > 0.0 {
                    (bytes.saturating_sub(prev_bytes)) as f64 / dt / (1024.0 * 1024.0)
                } else {
                    0.0
                };
                prev_bytes = bytes;
                prev_t = now;
                let done = progress.load(Ordering::Relaxed);
                let mib = bytes as f64 / (1024.0 * 1024.0);
                let mut err = std::io::stderr().lock();
                let _ = write!(
                    err,
                    "\r\x1b[2K{done}/{total} files | {mib:>8.1} MiB | {mbs:>7.1} MiB/s | permits={}",
                    dl_limit.target()
                );
                let _ = err.flush();
            }
            let _ = writeln!(std::io::stderr());
        }))
    } else {
        None
    };

    // Feed the pipeline.
    for rec in records {
        if rec_tx.send(rec).await.is_err() {
            break;
        }
    }
    drop(rec_tx);

    for h in crc_handles {
        h.await;
    }
    // CRC workers all dropped their dl_tx clones; download stage drains naturally.
    for h in dl_handles {
        h.await;
    }

    stop_signal.store(true, Ordering::Relaxed);
    controller.await;
    if let Some(r) = renderer {
        r.await;
    }

    Ok(PatchStats {
        total,
        downloaded: downloaded.load(Ordering::Relaxed),
        up_to_date: up_to_date.load(Ordering::Relaxed),
        skipped_missing: skipped_missing.load(Ordering::Relaxed),
        failed: failed.load(Ordering::Relaxed),
    })
}

struct DownloadJob {
    name: String,
    url: String,
    dest: PathBuf,
}

enum Decision {
    Download(PathBuf),
    UpToDate,
    SkippedMissing,
    SkippedPatchClient,
}

async fn classify(game_path: &Path, rec: &FileRecord, download_missing: bool) -> Decision {
    if rec
        .src_file_name
        .split('/')
        .next()
        .map(|s| s.eq_ignore_ascii_case("patchclient"))
        .unwrap_or(false)
    {
        return Decision::SkippedPatchClient;
    }

    let local_rel = fix_src_path(&rec.src_file_name);
    let local_path = game_path.join(local_rel);

    if !local_path.exists() {
        if download_missing {
            return Decision::Download(local_path);
        }
        return Decision::SkippedMissing;
    }
    match crc_of(local_path.clone()).await {
        Ok(local_crc) if local_crc as u64 == rec.crc => Decision::UpToDate,
        _ => Decision::Download(local_path),
    }
}

async fn resolve_urls(opts: &PatchOptions) -> Result<PatchUrls, WizPatchError> {
    let live = get_patch_urls(opts.game, opts.platform, opts.country).await?;
    let Some(target_rev) = &opts.revision else {
        return Ok(live);
    };
    let current_rev = revision_from_url(&live.file_list_url)?;
    Ok(PatchUrls {
        file_list_url: live.file_list_url.replace(&current_rev, target_rev),
        base_url: live.base_url.replace(&current_rev, target_rev),
    })
}

/// Streams a file through CRC32 in 64 KB chunks. Constant memory.
async fn crc_of(path: PathBuf) -> Result<u32, WizPatchError> {
    unblock(move || -> Result<u32, WizPatchError> {
        use std::io::Read;
        let mut f = std::fs::File::open(&path)?;
        let mut h = crc32fast::Hasher::new();
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            let n = f.read(&mut buf)?;
            if n == 0 {
                break;
            }
            h.update(&buf[..n]);
        }
        Ok(h.finalize())
    })
    .await
}

/// Adaptive semaphore. The controller bumps `target` up/down; permits drain
/// lazily as in-flight downloads release them, so we never cancel a transfer
/// mid-stream just to shrink.
struct AdaptiveLimit {
    sem: Arc<Semaphore>,
    /// Desired permit count.
    target: AtomicUsize,
    /// Permits currently in circulation (held or available in `sem`).
    issued: AtomicUsize,
    max: usize,
}

impl AdaptiveLimit {
    fn new(initial: usize, max: usize) -> Self {
        let initial = initial.max(1).min(max);
        Self {
            sem: Arc::new(Semaphore::new(initial)),
            target: AtomicUsize::new(initial),
            issued: AtomicUsize::new(initial),
            max,
        }
    }

    fn target(&self) -> usize {
        self.target.load(Ordering::Relaxed)
    }

    async fn acquire(self: &Arc<Self>) -> AdaptivePermit {
        let guard = self.sem.acquire_arc().await;
        AdaptivePermit {
            guard: Some(guard),
            limit: self.clone(),
        }
    }

    fn grow(&self, by: usize) {
        let cur = self.target.load(Ordering::Relaxed);
        let new_target = (cur + by).min(self.max);
        if new_target == cur {
            return;
        }
        self.target.store(new_target, Ordering::Relaxed);
        // Top up the in-circulation pool to match target.
        let issued = self.issued.load(Ordering::Relaxed);
        if new_target > issued {
            let add = new_target - issued;
            self.sem.add_permits(add);
            self.issued.fetch_add(add, Ordering::Relaxed);
        }
    }

    fn shrink(&self, by: usize) {
        let cur = self.target.load(Ordering::Relaxed);
        let new_target = cur.saturating_sub(by).max(1);
        self.target.store(new_target, Ordering::Relaxed);
        // Permits in excess of target are absorbed on permit drop.
    }

    fn shrink_half(&self) {
        let cur = self.target.load(Ordering::Relaxed);
        self.shrink(cur / 2);
    }
}

struct AdaptivePermit {
    guard: Option<SemaphoreGuardArc>,
    limit: Arc<AdaptiveLimit>,
}

impl Drop for AdaptivePermit {
    fn drop(&mut self) {
        // If we're over target, absorb this permit instead of releasing it.
        loop {
            let issued = self.limit.issued.load(Ordering::Relaxed);
            let target = self.limit.target.load(Ordering::Relaxed);
            if issued <= target {
                return; // drop guard normally — permit returns to sem
            }
            if self
                .limit
                .issued
                .compare_exchange(issued, issued - 1, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                if let Some(g) = self.guard.take() {
                    std::mem::forget(g);
                }
                return;
            }
        }
    }
}
