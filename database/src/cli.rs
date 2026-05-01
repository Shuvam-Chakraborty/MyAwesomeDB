use std::path::{Path, PathBuf};

#[derive(clap::Parser, Debug)]
pub struct CliOptions {
    #[arg(short, long)]
    config: PathBuf,
}

impl CliOptions {
    pub fn config_path(&self) -> &Path {
        self.config.as_path()
    }
}
