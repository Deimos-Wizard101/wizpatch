//! Stateless parallel patcher. Streams downloads and CRC checks; one shared
//! ureq agent gives connection reuse across all jobs.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

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
    /// Concurrent download workers.
    pub jobs: usize,
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
    println!("File list has {total} records. Running {} jobs.", opts.jobs);

    let agent = Arc::new(build_agent());
    let base_url = Arc::new(urls.base_url.clone());
    let game_path = Arc::new(opts.game_path.clone());

    let downloaded = Arc::new(AtomicUsize::new(0));
    let up_to_date = Arc::new(AtomicUsize::new(0));
    let skipped_missing = Arc::new(AtomicUsize::new(0));
    let failed = Arc::new(AtomicUsize::new(0));
    let progress = Arc::new(AtomicUsize::new(0));

    let download_missing = opts.download_missing;

    let (tx, rx) = async_channel::bounded::<FileRecord>(opts.jobs * 2);

    let mut workers = Vec::with_capacity(opts.jobs);
    for _ in 0..opts.jobs {
        let rx = rx.clone();
        let agent = agent.clone();
        let base_url = base_url.clone();
        let game_path = game_path.clone();
        let downloaded = downloaded.clone();
        let up_to_date = up_to_date.clone();
        let skipped_missing = skipped_missing.clone();
        let failed = failed.clone();
        let progress = progress.clone();
        workers.push(smol::spawn(async move {
            while let Ok(rec) = rx.recv().await {
                let outcome =
                    process_one(&agent, &base_url, &game_path, &rec, download_missing).await;
                let n = progress.fetch_add(1, Ordering::Relaxed) + 1;
                match outcome {
                    Outcome::Downloaded(bytes) => {
                        downloaded.fetch_add(1, Ordering::Relaxed);
                        println!(
                            "[{n:>5}/{total:>5}] {} ({} bytes)",
                            rec.src_file_name, bytes
                        );
                    }
                    Outcome::UpToDate => {
                        up_to_date.fetch_add(1, Ordering::Relaxed);
                    }
                    Outcome::SkippedMissing => {
                        skipped_missing.fetch_add(1, Ordering::Relaxed);
                    }
                    Outcome::SkippedPatchClient => {}
                    Outcome::Failed(e) => {
                        failed.fetch_add(1, Ordering::Relaxed);
                        eprintln!(
                            "[{n:>5}/{total:>5}] {} FAILED: {e}",
                            rec.src_file_name
                        );
                    }
                }
            }
        }));
    }
    drop(rx);

    for rec in records {
        if tx.send(rec).await.is_err() {
            break;
        }
    }
    drop(tx);

    for w in workers {
        w.await;
    }

    Ok(PatchStats {
        total,
        downloaded: downloaded.load(Ordering::Relaxed),
        up_to_date: up_to_date.load(Ordering::Relaxed),
        skipped_missing: skipped_missing.load(Ordering::Relaxed),
        failed: failed.load(Ordering::Relaxed),
    })
}

enum Outcome {
    Downloaded(u64),
    UpToDate,
    SkippedMissing,
    SkippedPatchClient,
    Failed(WizPatchError),
}

async fn process_one(
    agent: &ureq::Agent,
    base_url: &str,
    game_path: &Path,
    rec: &FileRecord,
    download_missing: bool,
) -> Outcome {
    if rec
        .src_file_name
        .split('/')
        .next()
        .map(|s| s.eq_ignore_ascii_case("patchclient"))
        .unwrap_or(false)
    {
        return Outcome::SkippedPatchClient;
    }

    let local_rel = fix_src_path(&rec.src_file_name);
    let local_path = game_path.join(local_rel);

    let exists = local_path.exists();
    if !exists {
        if !download_missing {
            return Outcome::SkippedMissing;
        }
    } else {
        match crc_of(local_path.clone()).await {
            Ok(local_crc) if local_crc as u64 == rec.crc => return Outcome::UpToDate,
            _ => {}
        }
    }

    let url = format!("{}/{}", base_url, rec.src_file_name);
    match download_to_file(agent, &url, local_path).await {
        Ok(bytes) => Outcome::Downloaded(bytes),
        Err(e) => Outcome::Failed(e),
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
