use std::{
    fs,
    process::{Command, Stdio},
};

fn test_directory() -> std::path::PathBuf {
    let directory = std::env::temp_dir().join(format!("sumi-dotenv-test-{}", uuid::Uuid::now_v7()));
    fs::create_dir(&directory).expect("create test directory");
    directory
}

#[test]
fn startup_loads_an_explicitly_selected_env_file() {
    let directory = test_directory();
    fs::write(
        directory.join(".env.local"),
        "SUMI_CONFIG=missing-from-dot-env.toml\n",
    )
    .expect("write .env.local");

    let output = Command::new(env!("CARGO_BIN_EXE_sumi-agent"))
        .current_dir(&directory)
        .env("SUMI_ENV_FILE", directory.join(".env.local"))
        .env_remove("SUMI_CONFIG")
        .env("SUMI_WORKSPACE", directory.join("workspace"))
        .env("SUMI_STATE_DIR", directory.join("state"))
        .env(
            "SUMI_AGENT_WRAPPING_KEY",
            "4242424242424242424242424242424242424242424242424242424242424242",
        )
        .stdin(Stdio::null())
        .output()
        .expect("run sumi-agent");

    fs::remove_dir_all(directory).expect("remove test directory");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("failed to read config file missing-from-dot-env.toml"),
        "unexpected stderr: {stderr}"
    );
}

#[test]
fn startup_does_not_implicitly_trust_dot_env_local_in_the_working_directory() {
    let directory = test_directory();
    fs::write(
        directory.join(".env.local"),
        "SUMI_CONFIG=missing-from-dot-env.toml\n",
    )
    .expect("write .env.local");

    let output = Command::new(env!("CARGO_BIN_EXE_sumi-agent"))
        .current_dir(&directory)
        .env_remove("SUMI_CONFIG")
        .env_remove("SUMI_ENV_FILE")
        .env(
            "SUMI_PERSONALITY_AGENT_ID",
            "0198f0f4-9b72-7000-8000-000000000001",
        )
        .env("SUMI_WORKSPACE", directory.join("workspace"))
        .env("SUMI_STATE_DIR", directory.join("state"))
        .env(
            "SUMI_AGENT_WRAPPING_KEY",
            "4242424242424242424242424242424242424242424242424242424242424242",
        )
        .stdin(Stdio::null())
        .output()
        .expect("run sumi-agent");

    fs::remove_dir_all(directory).expect("remove test directory");

    assert!(output.status.success());
}
