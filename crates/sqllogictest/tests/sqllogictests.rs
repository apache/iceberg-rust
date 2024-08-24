use std::fs;
use std::path::PathBuf;
use libtest_mimic::{Arguments, Trial};
use tokio::runtime::Handle;
use iceberg_test_utils::docker::DockerCompose;
use sqllogictest::schedule::Schedule;

fn main() {
    env_logger::init();

    log::info!("Starting docker compose...");
    let docker = start_docker().unwrap();

    log::info!("Starting tokio runtime...");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    // Parse command line arguments
    let args = Arguments::from_args();

    log::info!("Creating tests...");
    let tests = collect_trials(rt.handle().clone()).unwrap();

    log::info!("Starting tests...");
    // Run all tests and exit the application appropriatly.
    let result = libtest_mimic::run(&args, tests);

    log::info!("Shutting down tokio runtime...");
    drop(rt);
    log::info!("Shutting down docker...");
    drop(docker);

    result.exit();
}

fn start_docker() -> anyhow::Result<DockerCompose> {
    let docker = DockerCompose::new("sqllogictests",
                                    format!("{}/testdata/docker", env!("CARGO_MANIFEST_DIR")));
    docker.run();
    Ok(docker)
}

fn collect_trials(handle: Handle) -> anyhow::Result<Vec<Trial>> {
    let schedule_files = collect_schedule_files()?;
    log::debug!("Found {} schedule files: {}", schedule_files.len(), &schedule_files);
    let mut trials = Vec::with_capacity(schedule_files.len());
    for schedule_file in schedule_files {
        let h = handle.clone();
        let trial_name = format!("Test schedule {}",
                                 schedule_file.file_name()
                                     .expect("Schedule file should have a name")
                                     .to_string_lossy());
        let trial = Trial::new(trial_name, move || h.block_on(run_schedule(schedule_file.clone())));
        trials.push(trial);
    }
    Ok(trials)
}

fn collect_schedule_files() -> anyhow::Result<Vec<PathBuf>> {
    let dir = PathBuf::from(format!("{}/testdata/schedules", env!("CARGO_MANIFEST_DIR")));
    let mut schedule_files = Vec::with_capacity(32);
    for entry in fs::read_dir(&dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            schedule_files.push(fs::canonicalize(dir.join(path))?);
        }
    }
    Ok(schedule_files)
}

async fn run_schedule(schedule_file: PathBuf) -> anyhow::Result<()> {
    let schedule = Schedule::parse(schedule_file).await?;
    schedule.run().await?;
    Ok(())
}