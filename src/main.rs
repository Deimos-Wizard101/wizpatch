use std::path::PathBuf;
use std::process;

use wizpatch::{patch, Country, Game, PatchOptions, Platform, WizPatchError};

fn usage() {
    eprintln!(
        "wizpatch — Wizard101 patcher\n\
         \n\
         Usage: wizpatch [options] --path <DIR>\n\
         \n\
         Options:\n\
         \n\
           --path <DIR>             Game install directory (required)\n\
           --patch                  Run patch (default: on)\n\
           --no-patch               Do not patch (no-op invocation)\n\
           --country <us|eu>        Patch server region (default: us)\n\
           --revision <STR>         Pin a specific revision\n\
                                    (e.g. V_r800683.Wizard_1_610)\n\
           --download-missing       Download files missing locally (default: on)\n\
           --no-download-missing    Only update files already present\n\
           --jobs <N>               Parallel download workers (default: 5)\n\
           -h, --help               Print this help"
    );
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
    let mut jobs: usize = 5;

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

    let Some(game_path) = game_path else {
        eprintln!("Error: --path is required.");
        usage();
        process::exit(1);
    };

    let opts = PatchOptions {
        game: Game::Wizard101,
        platform: Platform::Windows,
        country,
        revision,
        download_missing,
        game_path,
        jobs,
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
