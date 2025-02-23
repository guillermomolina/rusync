use anyhow::Error;
use clap::Parser;
use log::{error, info, warn};
use rusync::sync::SyncOptions;
use rusync::Syncer;
use std::env;
use std::path::PathBuf;
use std::process;

const MAX_PARALLELISM: usize = 64;

#[derive(Debug, Parser)]
#[clap(name = "rusync")]
struct Opt {
    #[clap(
        long = "no-perms",
        help = "Do not preserve permissions (no-op on Windows)"
    )]
    no_preserve_permissions: bool,

    #[clap(long = "err-list", help = "Write errors to the given file")]
    error_list_path: Option<PathBuf>,

    #[clap(
        short = 'n',
        long = "dry-run",
        help = "Perform a trial run with no changes made"
    )]
    perform_trial_run: bool,

    #[clap(short = 'q', long = "quiet", help = "Suppress non-error messages")]
    no_show_progress: bool,

    #[clap(
        short = 'p',
        long = "parallelism",
        help = "Allow up to n sync jobs (default is the number of online processors)",
    )]
    parallelism: Option<usize>,

    #[clap(
        long = "log-level", 
        help = "Set the log level (e.g., info, debug, trace)"
    )]
    log_level: Option<String>,

    #[clap(parse(from_os_str))]
    source: PathBuf,

    #[clap(parse(from_os_str))]
    destination: PathBuf,
}

fn get_parallelism(opt: &Opt) -> usize {
    let available_parallelism = std::thread::available_parallelism().unwrap().get();
    if opt.parallelism.is_none() {
        available_parallelism
    } else {
        let parallelism = opt.parallelism.unwrap();
        if parallelism > available_parallelism {
            warn!("Requested parallelism is greater than available processors.");
        }
        if parallelism > MAX_PARALLELISM {
            error!("Requested parallelism is greater than {} processors.", MAX_PARALLELISM);
            process::exit(1);
        }
        parallelism
    }
}

fn main() -> Result<(), Error> {
    let opt = Opt::parse();

    // Set log level based on command-line argument or environment variable
    if let Some(log_level) = &opt.log_level {
        env::set_var("RUST_LOG", log_level);
    }
    env_logger::init();

    info!("Starting rusync");
    let source = &opt.source;
    if !source.is_dir() {
        eprintln!("{} is not a directory", source.to_string_lossy());
        process::exit(1);
    }
    let destination = &opt.destination;

    let options = SyncOptions {
        preserve_permissions: !opt.no_preserve_permissions,
        perform_dry_run: opt.perform_trial_run,
        parallelism: get_parallelism(&opt),
    };
    let syncer = Syncer::new(source, destination, options);
    let stats = syncer.sync();
    match stats {
        Err(err) => {
            eprintln!("{}", err);
            process::exit(1);
        }
        Ok(stats) if stats.errors > 0 => {
            process::exit(1);
        }
        _ => {
            process::exit(0);
        }
    }
}
