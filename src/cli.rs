use clap::{ArgAction, Parser};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(
    name = "Prometheus websocket proxy",
    version,
    author = "Roman Karpovich <fpm.th13f@gmail.com>",
    about = "Connects to websocket server to call local resources"
)]
pub struct Cli {
    #[arg(default_value = "client_config.json", help = "path to config")]
    pub config: PathBuf,

    #[arg(
        short = 'p',
        long = "parallel",
        default_value_t = 3,
        value_parser = parse_parallel,
        help = "number of connections to use"
    )]
    pub parallel: usize,

    #[arg(
        short = 'r',
        long = "protocol",
        default_value_t = 3,
        value_parser = parse_protocol,
        help = "historical wire protocol version"
    )]
    pub protocol: u8,

    #[arg(long = "sentry_dsn", value_name = "DSN")]
    pub sentry_dsn: Option<String>,

    #[arg(short = 'v', long = "verbose", action = ArgAction::Count)]
    pub verbose: u8,
}

fn parse_parallel(value: &str) -> Result<usize, String> {
    let parallel = value
        .parse::<usize>()
        .map_err(|_| "parallel must be a positive integer".to_owned())?;
    if parallel == 0 {
        Err("parallel must be at least 1".to_owned())
    } else {
        Ok(parallel)
    }
}

fn parse_protocol(value: &str) -> Result<u8, String> {
    match value.parse::<u8>() {
        Ok(protocol @ 1..=3) => Ok(protocol),
        _ => Err("protocol must be one of 1, 2, or 3".to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::Cli;
    use clap::Parser;
    use std::path::PathBuf;

    #[test]
    fn defaults_match_the_systemd_contract() {
        let cli = Cli::try_parse_from(["proxy-client"]).unwrap();
        assert_eq!(cli.config, PathBuf::from("client_config.json"));
        assert_eq!(cli.parallel, 3);
        assert_eq!(cli.protocol, 3);
        assert_eq!(cli.verbose, 0);
    }

    #[test]
    fn accepts_current_and_python_compatible_forms() {
        let cli = Cli::try_parse_from([
            "proxy-client",
            "/etc/prometheus/client.json",
            "--parallel=3",
            "--protocol=2",
            "-vv",
            "--sentry_dsn=https://public@example.invalid/1",
        ])
        .unwrap();
        assert_eq!(cli.config, PathBuf::from("/etc/prometheus/client.json"));
        assert_eq!(cli.parallel, 3);
        assert_eq!(cli.protocol, 2);
        assert_eq!(cli.verbose, 2);
        assert!(cli.sentry_dsn.is_some());
    }

    #[test]
    fn rejects_unknown_wire_protocol() {
        assert!(Cli::try_parse_from(["proxy-client", "--protocol=4"]).is_err());
    }
}
