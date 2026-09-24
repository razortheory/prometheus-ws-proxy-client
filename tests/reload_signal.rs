#![cfg(unix)]

use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

struct Client(Child);

impl Drop for Client {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
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
    // The reload handler is installed before logging starts, so the first log
    // line means the signal can be delivered safely.
    let (lines_tx, lines) = mpsc::channel();
    let stdout = client.0.stdout.take().unwrap();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            let _ = lines_tx.send(line);
        }
    });
    let first = lines
        .recv_timeout(Duration::from_secs(20))
        .expect("client did not start");
    assert!(first.contains("loading configuration"), "{first}");

    signal(&client.0, "HUP");
    let logged_by = Instant::now() + Duration::from_secs(5);
    while !lines
        .recv_timeout(Duration::from_millis(100))
        .is_ok_and(|line| line.contains("SIGHUP received"))
    {
        assert!(
            client.0.try_wait().unwrap().is_none(),
            "SIGHUP stopped the client"
        );
        assert!(Instant::now() < logged_by, "SIGHUP was not logged");
    }
    assert!(client.0.try_wait().unwrap().is_none());

    signal(&client.0, "TERM");
    let deadline = Instant::now() + Duration::from_secs(5);
    let status = loop {
        if let Some(status) = client.0.try_wait().unwrap() {
            break status;
        }
        assert!(Instant::now() < deadline, "SIGTERM did not stop the client");
        std::thread::sleep(Duration::from_millis(50));
    };
    assert!(status.success());
    let _ = std::fs::remove_file(config);
}
