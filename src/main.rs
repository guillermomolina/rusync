use anyhow::Error;
use clap::Parser;
use rusync::console_info::ConsoleProgressInfo;
use rusync::sync::SyncOptions;
use rusync::Syncer;
use std::path::PathBuf;
use std::process;

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
        default_value_t = 0
    )]
    parallelism: usize,

    #[clap(parse(from_os_str))]
    source: PathBuf,

    #[clap(parse(from_os_str))]
    destination: PathBuf,
}

fn get_parallelism(parallelism: usize) -> usize {
    let available_parallelism = std::thread::available_parallelism().unwrap().get();
    if parallelism == 0 || parallelism > available_parallelism {
        available_parallelism
    } else {
        parallelism
    }
}

fn main() -> Result<(), Error> {
    let opt = Opt::parse();
    let source = &opt.source;
    if !source.is_dir() {
        eprintln!("{} is not a directory", source.to_string_lossy());
        process::exit(1);
    }
    let destination = &opt.destination;

    let console_info = match opt.error_list_path {
        Some(err_file) => {
            ConsoleProgressInfo::with_error_list_path(!opt.no_show_progress, &err_file)?
        }
        None => ConsoleProgressInfo::new(!opt.no_show_progress),
    };
    let options = SyncOptions {
        preserve_permissions: !opt.no_preserve_permissions,
        perform_dry_run: opt.perform_trial_run,
        parallelism: get_parallelism(opt.parallelism),
    };
    let syncer = Syncer::new(source, destination, options, Box::new(console_info));
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
