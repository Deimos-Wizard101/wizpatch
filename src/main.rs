use std::path::PathBuf;
use std::process;

use wizpatch::{patch, Country, Game, PatchOptions, Platform, WizPatchError};

fn usage() {
    eprintln!(
        "wizpatch — Wizard101 patcher\n\
         \n\
         Usage: wizpatch [options]\n\
         \n\
         Options:\n\
         \n\
           --path <DIR>             Game install directory\n\
                                    (auto-detected on macOS/Windows; required on Linux)\n\
           --platform <p>           windows|mac|steam\n\
                                    (default: mac on macOS, windows elsewhere)\n\
           --patch                  Run patch (default: on)\n\
           --no-patch               Do not patch (no-op invocation)\n\
           --country <us|eu>        Patch server region (default: us)\n\
           --revision <STR>         Pin a specific revision\n\
                                    (e.g. V_r800683.Wizard_1_610)\n\
           --download-missing       Download files missing locally (default: on)\n\
           --no-download-missing    Only update files already present\n\
           --jobs <N>               Max parallel download workers (default: 5)\n\
           -v, --verbose            Print per-file completions and controller stats\n\
           -h, --help               Print this help"
    );
}

fn default_platform() -> Platform {
    if cfg!(target_os = "macos") {
        Platform::MacOs
    } else {
        Platform::Windows
    }
}

fn default_game_path(platform: Platform) -> Option<PathBuf> {
    let candidates: Vec<PathBuf> = if cfg!(target_os = "macos") {
        vec![PathBuf::from("/Applications/Wizard101")]
    } else if cfg!(target_os = "windows") {
        let non_steam = PathBuf::from(r"C:\ProgramData\KingsIsle Entertainment\Wizard101");
        let steam = PathBuf::from(r"C:\Program Files (x86)\Steam\steamapps\common\Wizard101");
        match platform {
            Platform::Steam => vec![steam, non_steam],
            _ => vec![non_steam, steam],
        }
    } else {
        return None;
    };
    candidates.into_iter().find(|p| p.is_dir())
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if let Err(e) = smol::block_on(run(args)) {
        eprintln!("Error: {e}");
        process::exit(1);
    }
}

async fn run(args: Vec<String>) -> Result<(), WizPatchError> {
    let mut do_patch = true;
    let mut download_missing = true;
    let mut country = Country::Us;
    let mut revision: Option<String> = None;
    let mut game_path: Option<PathBuf> = None;
    let mut platform: Option<Platform> = None;
    let mut jobs: usize = 5;
    let mut verbose = false;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--patch" => do_patch = true,
            "--no-patch" => do_patch = false,
            "--download-missing" => download_missing = true,
            "--no-download-missing" => download_missing = false,
            "--country" => {
                i += 1;
                country = match args.get(i).map(String::as_str) {
                    Some("us") => Country::Us,
                    Some("eu") => Country::Eu,
                    other => {
                        return Err(WizPatchError::Protocol(format!(
                            "unknown country: {other:?}"
                        )));
                    }
                };
            }
            "--platform" => {
                i += 1;
                platform = Some(match args.get(i).map(String::as_str) {
                    Some("windows") => Platform::Windows,
                    Some("mac") | Some("macos") => Platform::MacOs,
                    Some("steam") => Platform::Steam,
                    other => {
                        return Err(WizPatchError::Protocol(format!(
                            "unknown platform: {other:?}"
                        )));
                    }
                });
            }
            "--revision" => {
                i += 1;
                revision = Some(
                    args.get(i)
                        .ok_or_else(|| {
                            WizPatchError::Protocol("--revision needs a value".into())
                        })?
                        .clone(),
                );
            }
            "--path" => {
                i += 1;
                game_path = Some(PathBuf::from(args.get(i).ok_or_else(|| {
                    WizPatchError::Protocol("--path needs a value".into())
                })?));
            }
            "--jobs" => {
                i += 1;
                jobs = args
                    .get(i)
                    .and_then(|s| s.parse().ok())
                    .filter(|&n: &usize| n >= 1)
                    .ok_or_else(|| {
                        WizPatchError::Protocol("--jobs needs a positive integer".into())
                    })?;
            }
            "-v" | "--verbose" => verbose = true,
            "-h" | "--help" | "help" => {
                usage();
                return Ok(());
            }
            other => {
                eprintln!("Unknown argument: {other}");
                usage();
                process::exit(1);
            }
        }
        i += 1;
    }

    if !do_patch {
        println!("--no-patch given; nothing to do.");
        return Ok(());
    }

    let platform = platform.unwrap_or_else(default_platform);

    let game_path = match game_path {
        Some(p) => p,
        None => match default_game_path(platform) {
            Some(p) => {
                println!("Auto-detected install: {}", p.display());
                p
            }
            None => {
                eprintln!(
                    "Error: could not auto-detect a Wizard101 install on this OS — pass --path <DIR>."
                );
                process::exit(1);
            }
        },
    };

    let opts = PatchOptions {
        game: Game::Wizard101,
        platform,
        country,
        revision,
        download_missing,
        game_path,
        jobs,
        verbose,
    };

    let stats = patch(&opts).await?;
    println!(
        "\nDone. {} downloaded, {} up-to-date, {} skipped (missing), {} failed (of {} records).",
        stats.downloaded,
        stats.up_to_date,
        stats.skipped_missing,
        stats.failed,
        stats.total
    );
    Ok(())
}
