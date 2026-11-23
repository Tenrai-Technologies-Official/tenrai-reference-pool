use std::process::ExitCode;
use tenrai_pool::{
    cli,
    config::Config,
    console::{self, LogOptions},
};

#[tokio::main]
async fn main() -> ExitCode {
    let cli = match cli::parse(std::env::args_os().collect()) {
        Ok(cli) => cli,
        Err(error) => match error.downcast::<clap::Error>() {
            Ok(error) => error.exit(),
            Err(error) => {
                eprintln!("{error:#}");
                return ExitCode::FAILURE;
            }
        },
    };
    if let Err(error) = console::init(LogOptions {
        debug: cli.debug,
        color: !cli.no_color,
        file: cli.log_file.as_deref(),
    }) {
        eprintln!("cannot start logging: {error}");
        return ExitCode::FAILURE;
    }
    let result = match Config::from_cli(&cli) {
        Ok(config) => tenrai_pool::run(config).await,
        Err(error) => Err(error),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!("{error:#}");
            ExitCode::FAILURE
        }
    }
}
