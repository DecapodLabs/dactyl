//! Convert a SQLite database into a Dactyl snapshot without a caller SQLite link.

use std::path::PathBuf;
use std::process::ExitCode;

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let Some(source) = args.next() else {
        eprintln!("usage: dactyl-import <source> [destination]");
        return ExitCode::from(2);
    };
    let destination = args.next().unwrap_or_else(|| source.clone());
    match dactyl_db::import_sqlite_file(PathBuf::from(source), PathBuf::from(destination)) {
        Ok(report) => match serde_json::to_string_pretty(&report) {
            Ok(json) => {
                println!("{json}");
                ExitCode::SUCCESS
            }
            Err(error) => {
                eprintln!("encode report: {error}");
                ExitCode::from(1)
            }
        },
        Err(error) => {
            eprintln!("{error}");
            ExitCode::from(1)
        }
    }
}
