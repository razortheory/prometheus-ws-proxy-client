#![cfg(unix)]

use std::io::{BufRead, BufReader};
use std::os::unix::process::ExitStatusExt;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::time::{Duration, Instant};

struct Client(Child);

impl Drop for Client {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn describe(status: ExitStatus) -> String {
    match status.signal() {
        Some(signal) => format!("{status} (killed by signal {signal})"),
        None => status.to_string(),
    }
}

/// Waits for a log line containing `needle`, failing if the client exits.
fn wait_for_line(client: &mut Child, lines: &mpsc::Receiver<String>, needle: &str) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        match lines.recv_timeout(Duration::from_millis(100)) {
            Ok(line) if line.contains(needle) => return,
            Ok(_) | Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => {
                let status = client.wait().unwrap();
                panic!(
                    "client exited before logging {needle:?}: {}",
                    describe(status)
                );
            }
        }
        if let Some(status) = client.try_wait().unwrap() {
            panic!(
                "client exited before logging {needle:?}: {}",
                describe(status)
            );
        }
        assert!(Instant::now() < deadline, "client never logged {needle:?}");
    }
}

fn signal(child: &Child, name: &str) {
    let status = Command::new("kill")
        .arg(format!("-{name}"))
        .arg(child.id().to_string())
        .status()
        .expect("kill is available");
    assert!(status.success());
}

#[test]
fn sighup_is_ignored_and_sigterm_still_stops_the_client() {
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let config = std::env::temp_dir().join(format!("proxy-client-reload-{port}.json"));
    std::fs::write(
        &config,
        format!(
            r#"{{"instance":"reload-test","target":"http://127.0.0.1:{port}/proxy/","resources":{{}}}}"#
        ),
    )
    .unwrap();

    let mut client = Client(
        Command::new(env!("CARGO_BIN_EXE_proxy-client"))
            .arg(&config)
            .arg("--parallel=1")
            .arg("-v")
            .env_remove("RUST_LOG")
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let (lines_tx, lines) = mpsc::channel();
    let stdout = client.0.stdout.take().unwrap();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            let _ = lines_tx.send(line);
        }
    });
    // Logged only after every signal handler is registered.
    wait_for_line(&mut client.0, &lines, "signal handlers installed");

    signal(&client.0, "HUP");
    wait_for_line(&mut client.0, &lines, "SIGHUP received");

    signal(&client.0, "TERM");
    let deadline = Instant::now() + Duration::from_secs(5);
    let status = loop {
        if let Some(status) = client.0.try_wait().unwrap() {
            break status;
        }
        assert!(Instant::now() < deadline, "SIGTERM did not stop the client");
        std::thread::sleep(Duration::from_millis(50));
    };
    // A graceful shutdown returns from main; the default SIGTERM action
    // would end the process by signal instead.
    assert_eq!(status.code(), Some(0), "{}", describe(status));
    let _ = std::fs::remove_file(config);
}
