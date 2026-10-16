use clap::Parser;
use std::path::PathBuf;

#[allow(dead_code)]
#[path = "credential_probe.rs"]
mod credential_probe;
mod legacy {
    include!("usage_probe.rs");
    pub(super) fn run() {
        main();
    }
}
#[derive(Parser, Debug)]
#[command(
    name = "zed-native-usage-probe",
    about = "Read native usage once; factory credentials require explicit namespace and pinned identity"
)]
struct Args {
    #[arg(long)]
    user_data_dir: PathBuf,
    #[arg(long, requires = "expected_identity", value_parser = [openai_subscribed::native_auth::FACTORY_CREDENTIAL_NAMESPACE])]
    credential_namespace: Option<String>,
    #[arg(long, requires = "credential_namespace")]
    expected_identity: Option<String>,
}
fn main() {
    let args = Args::parse();
    if args.credential_namespace.is_some() {
        credential_probe::run(true);
    } else {
        legacy::run();
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn scoped_namespace_parser_accepts_only_alias() {
        for namespace in [
            "factory-openai",
            "https://factory.zed.native/openai-subscribed/v1",
            "arbitrary-key",
        ] {
            assert_eq!(
                Args::try_parse_from([
                    "usage",
                    "--user-data-dir",
                    "/fixture",
                    "--credential-namespace",
                    namespace,
                    "--expected-identity",
                    "opaque",
                ])
                .is_ok(),
                namespace == "factory-openai"
            );
        }
    }
    #[test]
    fn legacy_flags_and_explicit_scoped_pair() {
        let args = Args::try_parse_from(["usage", "--user-data-dir", "/fixture"]).unwrap();
        assert!(args.credential_namespace.is_none());
        assert!(
            Args::try_parse_from([
                "usage",
                "--user-data-dir",
                "/fixture",
                "--expected-identity",
                "opaque"
            ])
            .is_err()
        );
        assert!(
            Args::try_parse_from([
                "usage",
                "--user-data-dir",
                "/fixture",
                "--credential-namespace",
                "factory"
            ])
            .is_err()
        );
        assert!(
            Args::try_parse_from(["usage", "--user-data-dir", "/fixture", "--sign-in"]).is_err()
        );
    }
}
