use anyhow::{bail, Context, Result};
use figment::{
    providers::{Env, Format, Serialized, Toml},
    Figment,
};
use serde::{Deserialize, Serialize};

/// Environment variable prefix for backend-specific settings.
const BACKEND_ENV_PREFIX: &str = "LEXE_";
const BACKEND_CONFIG_SECTION: &str = "lexe";

/// Networks accepted by the processor.
const NETWORKS: &[&str] = &["mainnet", "testnet3", "regtest"];

/// Backend-specific configuration for the Lexe managed node.
///
/// Credentials follow the same model as lexe-mcp: a single base64
/// "client credentials" blob created in the Lexe app (Menu → SDK clients).
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BackendConfig {
    /// Base64 Lexe SDK client credentials (Lexe app → Menu → SDK clients).
    ///
    /// Primary credential, the same blob lexe-mcp passes to the sidecar as
    /// `Authorization: Bearer <credentials>`. Exactly one of this or
    /// `seed_phrase` must be set.
    #[serde(default)]
    pub client_credentials: Option<String>,
    /// BIP39 seed phrase for the Lexe node.
    ///
    /// Optional fallback for operators who provision the node directly from
    /// the seed instead of an SDK client.
    #[serde(default)]
    pub seed_phrase: Option<String>,
    /// Lexe environment: `mainnet`, `testnet3` or `regtest`.
    #[serde(default = "default_network")]
    pub network: String,
    /// Data directory for the Lexe wallet state and the quote database.
    #[serde(default = "default_data_dir")]
    pub data_dir: String,
    /// Estimated outgoing fee in ppm used for melt quotes. Lexe has no
    /// fee-estimation API, so quotes use this estimate; the actual fee is
    /// reported once the payment settles.
    #[serde(default = "default_fee_reserve_ppm")]
    pub fee_reserve_ppm: u32,
    /// Minimum estimated outgoing fee in sats.
    #[serde(default = "default_fee_reserve_min_sat")]
    pub fee_reserve_min_sat: u32,
    /// Default timeout in seconds for outgoing payments when the mint does
    /// not provide one. Lexe preflight can take ~60s, so keep this well
    /// above that.
    #[serde(default = "default_payment_timeout_secs")]
    pub payment_timeout_secs: u64,
}

fn default_network() -> String {
    "mainnet".to_string()
}

fn default_data_dir() -> String {
    ".data/lexe".to_string()
}

fn default_fee_reserve_ppm() -> u32 {
    100
}

fn default_fee_reserve_min_sat() -> u32 {
    1
}

fn default_payment_timeout_secs() -> u64 {
    300
}

impl Default for BackendConfig {
    fn default() -> Self {
        Self {
            client_credentials: None,
            seed_phrase: None,
            network: default_network(),
            data_dir: default_data_dir(),
            fee_reserve_ppm: default_fee_reserve_ppm(),
            fee_reserve_min_sat: default_fee_reserve_min_sat(),
            payment_timeout_secs: default_payment_timeout_secs(),
        }
    }
}

/// Main configuration: config.toml overlaid by environment variables.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Config {
    /// Lexe-specific configuration
    #[serde(default)]
    pub lexe: BackendConfig,
    /// gRPC server address.
    #[serde(default = "default_address")]
    pub address: String,
    /// gRPC server port.
    #[serde(default = "default_port")]
    pub port: u16,
    /// TLS for the payment processor gRPC server.
    #[serde(default)]
    pub tls_enable: bool,
    /// Explicitly allow plaintext gRPC.
    #[serde(default)]
    pub allow_insecure: bool,
    #[serde(default = "default_tls_cert_path")]
    pub tls_cert_path: String,
    #[serde(default = "default_tls_key_path")]
    pub tls_key_path: String,
    /// PEM CA certificate used to authenticate mint clients.
    #[serde(default = "default_tls_client_ca_path")]
    pub tls_client_ca_path: String,
}

fn default_address() -> String {
    "127.0.0.1".to_string()
}

fn default_port() -> u16 {
    50051
}

fn default_tls_cert_path() -> String {
    "certs/server.crt".to_string()
}

fn default_tls_key_path() -> String {
    "certs/server.key".to_string()
}

fn default_tls_client_ca_path() -> String {
    "certs/ca.pem".to_string()
}

impl Default for Config {
    fn default() -> Self {
        Self {
            lexe: BackendConfig::default(),
            address: default_address(),
            port: default_port(),
            tls_enable: false,
            allow_insecure: false,
            tls_cert_path: default_tls_cert_path(),
            tls_key_path: default_tls_key_path(),
            tls_client_ca_path: default_tls_client_ca_path(),
        }
    }
}

impl Config {
    /// Load from config.toml (if present) and environment variables.
    /// Environment variables override file values.
    pub fn load() -> Result<Self> {
        let cfg = extract_config(config_figment())?;
        validate(&cfg)?;
        Ok(cfg)
    }

    /// Alias for [`Self::load`].
    pub fn from_env() -> Result<Self> {
        Self::load()
    }
}

fn config_figment() -> Figment {
    let mut figment = Figment::from(Serialized::defaults(Config::default()));
    if std::path::Path::new("config.toml").is_file() {
        figment = figment.merge(Toml::file_exact("config.toml"));
    }

    figment
        .merge(Env::prefixed("SERVER_"))
        .merge(Env::prefixed("TLS_").map(|key| format!("tls_{}", key.as_str()).into()))
        .merge(Env::raw().only(&["ALLOW_INSECURE"]))
        .merge(
            Env::prefixed(BACKEND_ENV_PREFIX)
                .map(|key| format!("{BACKEND_CONFIG_SECTION}.{}", key.as_str()).into()),
        )
}

fn extract_config(figment: Figment) -> Result<Config> {
    figment.extract().context("failed to parse configuration")
}

/// Validate the loaded configuration.
pub fn validate(cfg: &Config) -> Result<()> {
    let has_credentials = cfg
        .lexe
        .client_credentials
        .as_deref()
        .map(str::trim)
        .is_some_and(|s| !s.is_empty());
    let has_seed = cfg
        .lexe
        .seed_phrase
        .as_deref()
        .map(str::trim)
        .is_some_and(|s| !s.is_empty());

    match (has_credentials, has_seed) {
        (true, false) | (false, true) => {}
        (true, true) => {
            bail!("set either LEXE_CLIENT_CREDENTIALS or LEXE_SEED_PHRASE, not both");
        }
        (false, false) => {
            bail!(
                "missing credentials: set LEXE_CLIENT_CREDENTIALS (base64 SDK client \
                 credentials from the Lexe app, Menu → SDK clients) or LEXE_SEED_PHRASE (BIP39)"
            );
        }
    }

    let network = cfg.lexe.network.to_ascii_lowercase();
    if !NETWORKS.contains(&network.as_str()) {
        bail!(
            "invalid LEXE_NETWORK `{}`; expected one of: {}",
            cfg.lexe.network,
            NETWORKS.join(", ")
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, MutexGuard};

    /// Process-wide lock: settings tests mutate environment variables.
    fn env_lock() -> MutexGuard<'static, ()> {
        static LOCK: Mutex<()> = Mutex::new(());
        LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Clear every LEXE_*/SERVER_*/TLS_*/ALLOW_INSECURE variable the tests use.
    fn clear_test_env() {
        for key in [
            "LEXE_CLIENT_CREDENTIALS",
            "LEXE_SEED_PHRASE",
            "LEXE_NETWORK",
            "LEXE_DATA_DIR",
            "LEXE_FEE_RESERVE_PPM",
            "LEXE_FEE_RESERVE_MIN_SAT",
            "LEXE_PAYMENT_TIMEOUT_SECS",
            "SERVER_ADDRESS",
            "SERVER_PORT",
            "TLS_ENABLE",
            "TLS_CERT_PATH",
            "TLS_KEY_PATH",
            "TLS_CLIENT_CA_PATH",
            "ALLOW_INSECURE",
        ] {
            std::env::remove_var(key);
        }
    }

    #[test]
    fn load_defaults_without_credentials_fails_validation() {
        let _guard = env_lock();
        clear_test_env();

        let err = Config::load().unwrap_err();
        assert!(
            err.to_string().contains("missing credentials"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn client_credentials_accepted() {
        let _guard = env_lock();
        clear_test_env();
        std::env::set_var("LEXE_CLIENT_CREDENTIALS", "dGVzdA==");

        let cfg = Config::load().expect("should load with client credentials");
        assert_eq!(cfg.lexe.client_credentials.as_deref(), Some("dGVzdA=="));
        assert_eq!(cfg.lexe.network, "mainnet");
        assert_eq!(cfg.lexe.data_dir, ".data/lexe");
        assert_eq!(cfg.lexe.fee_reserve_ppm, 100);
        assert_eq!(cfg.lexe.fee_reserve_min_sat, 1);
        assert_eq!(cfg.lexe.payment_timeout_secs, 300);
        clear_test_env();
    }

    #[test]
    fn seed_phrase_accepted() {
        let _guard = env_lock();
        clear_test_env();
        std::env::set_var("LEXE_SEED_PHRASE", "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about");

        let cfg = Config::load().expect("should load with seed phrase");
        assert!(cfg.lexe.client_credentials.is_none());
        assert!(cfg.lexe.seed_phrase.is_some());
        clear_test_env();
    }

    #[test]
    fn both_credentials_rejected() {
        let _guard = env_lock();
        clear_test_env();
        std::env::set_var("LEXE_CLIENT_CREDENTIALS", "dGVzdA==");
        std::env::set_var("LEXE_SEED_PHRASE", "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about");

        let err = Config::load().unwrap_err();
        assert!(
            err.to_string().contains("not both"),
            "unexpected error: {err}"
        );
        clear_test_env();
    }

    #[test]
    fn invalid_network_rejected() {
        let _guard = env_lock();
        clear_test_env();
        std::env::set_var("LEXE_CLIENT_CREDENTIALS", "dGVzdA==");
        std::env::set_var("LEXE_NETWORK", "signet");

        let err = Config::load().unwrap_err();
        assert!(
            err.to_string().contains("invalid LEXE_NETWORK"),
            "unexpected error: {err}"
        );
        clear_test_env();
    }

    #[test]
    fn env_overrides_defaults() {
        let _guard = env_lock();
        clear_test_env();
        std::env::set_var("LEXE_CLIENT_CREDENTIALS", "dGVzdA==");
        std::env::set_var("LEXE_NETWORK", "testnet3");
        std::env::set_var("LEXE_DATA_DIR", "/tmp/lexe-data");
        std::env::set_var("LEXE_FEE_RESERVE_PPM", "250");
        std::env::set_var("SERVER_PORT", "60001");
        std::env::set_var("ALLOW_INSECURE", "true");

        let cfg = Config::load().expect("should load with env overrides");
        assert_eq!(cfg.lexe.network, "testnet3");
        assert_eq!(cfg.lexe.data_dir, "/tmp/lexe-data");
        assert_eq!(cfg.lexe.fee_reserve_ppm, 250);
        assert_eq!(cfg.port, 60001);
        assert!(cfg.allow_insecure);
        clear_test_env();
    }

    #[test]
    fn invalid_port_rejected() {
        let _guard = env_lock();
        clear_test_env();
        std::env::set_var("LEXE_CLIENT_CREDENTIALS", "dGVzdA==");
        std::env::set_var("SERVER_PORT", "not-a-port");

        assert!(Config::load().is_err());
        clear_test_env();
    }
}
