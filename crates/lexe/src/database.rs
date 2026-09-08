//! Lexe quote database.
//!
//! Maps BOLT11 payment hashes to invoice strings and Lexe payment indexes so
//! that repeated `make_payment` / status checks never submit a payment twice.
//!
//! Lexe's `pay_invoice` blocks until a terminal state and has no client-side
//! idempotency token, so the Lexe payment index (created per invoice) is the
//! single source of retry safety.

use anyhow::Result;
use redb::{Database, ReadableDatabase, TableDefinition};
use std::path::Path;
use std::sync::Arc;

/// BOLT11 invoices we created, keyed by payment hash.
const MINT_QUOTES_TABLE: TableDefinition<&[u8; 32], &str> = TableDefinition::new("mint_quotes");

/// Lexe payment index (string form of `PaymentCreatedIndex`) for each created
/// invoice, keyed by payment hash.
const MINT_PAYMENT_IDS_TABLE: TableDefinition<&[u8; 32], &str> =
    TableDefinition::new("mint_payment_ids");

/// Outgoing BOLT11 invoices we quoted, keyed by payment hash.
const MELT_QUOTES_TABLE: TableDefinition<&[u8; 32], &str> = TableDefinition::new("melt_quotes");

/// Lexe payment index for each outgoing payment, keyed by payment hash.
const MELT_PAYMENT_IDS_TABLE: TableDefinition<&[u8; 32], &str> =
    TableDefinition::new("melt_payment_ids");

/// CDK melt quote id + quote unit, keyed by payment hash, as JSON
/// `{"quote_id": "...", "unit": "sat"|"msat"|...}` for event correlation.
const MELT_QUOTE_IDS_TABLE: TableDefinition<&[u8; 32], &str> =
    TableDefinition::new("melt_quote_ids");

/// Database wrapper for quote-to-payment mappings.
#[derive(Clone)]
pub struct QuoteDatabase {
    db: Arc<Database>,
}

impl QuoteDatabase {
    /// Create a new database instance or open an existing one.
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let db = Database::create(path)?;

        let write_txn = db.begin_write()?;
        {
            let _mint_quotes = write_txn.open_table(MINT_QUOTES_TABLE)?;
            let _mint_payment_ids = write_txn.open_table(MINT_PAYMENT_IDS_TABLE)?;
            let _melt_quotes = write_txn.open_table(MELT_QUOTES_TABLE)?;
            let _melt_payment_ids = write_txn.open_table(MELT_PAYMENT_IDS_TABLE)?;
            let _melt_quote_ids = write_txn.open_table(MELT_QUOTE_IDS_TABLE)?;
        }
        write_txn.commit()?;

        tracing::info!("Lexe quote database initialized");

        Ok(Self { db: Arc::new(db) })
    }

    /// Store a created mint invoice.
    pub fn insert_mint_quote(&self, payment_hash: &[u8; 32], payment_request: &str) -> Result<()> {
        self.insert_mapping(MINT_QUOTES_TABLE, payment_hash, payment_request)
    }

    /// Get the mint invoice we created for this payment hash.
    pub fn get_mint_quote(&self, payment_hash: &[u8; 32]) -> Result<Option<String>> {
        self.get_mapping(MINT_QUOTES_TABLE, payment_hash)
    }

    /// Store the Lexe payment index for a created invoice.
    pub fn insert_mint_payment_id(
        &self,
        payment_hash: &[u8; 32],
        payment_index: &str,
    ) -> Result<()> {
        self.insert_mapping(MINT_PAYMENT_IDS_TABLE, payment_hash, payment_index)
    }

    /// Get the Lexe payment index for a created invoice.
    pub fn get_mint_payment_id(&self, payment_hash: &[u8; 32]) -> Result<Option<String>> {
        self.get_mapping(MINT_PAYMENT_IDS_TABLE, payment_hash)
    }

    /// Store an outgoing (melt) invoice.
    pub fn insert_melt_quote(&self, payment_hash: &[u8; 32], payment_request: &str) -> Result<()> {
        self.insert_mapping(MELT_QUOTES_TABLE, payment_hash, payment_request)
    }

    /// Get the stored outgoing invoice for this payment hash.
    pub fn get_melt_quote(&self, payment_hash: &[u8; 32]) -> Result<Option<String>> {
        self.get_mapping(MELT_QUOTES_TABLE, payment_hash)
    }

    /// Store the Lexe payment index for an outgoing payment.
    pub fn insert_melt_payment_id(
        &self,
        payment_hash: &[u8; 32],
        payment_index: &str,
    ) -> Result<()> {
        self.insert_mapping(MELT_PAYMENT_IDS_TABLE, payment_hash, payment_index)
    }

    /// Get the Lexe payment index for an outgoing payment.
    pub fn get_melt_payment_id(&self, payment_hash: &[u8; 32]) -> Result<Option<String>> {
        self.get_mapping(MELT_PAYMENT_IDS_TABLE, payment_hash)
    }

    /// Store the CDK melt quote id + unit as JSON for event correlation.
    pub fn insert_melt_quote_id(
        &self,
        payment_hash: &[u8; 32],
        quote_id: &str,
        unit: &str,
    ) -> Result<()> {
        let value = serde_json::json!({ "quote_id": quote_id, "unit": unit }).to_string();
        self.insert_mapping(MELT_QUOTE_IDS_TABLE, payment_hash, &value)
    }

    /// Get the stored CDK melt quote id + unit JSON for this payment hash.
    pub fn get_melt_quote_id(&self, payment_hash: &[u8; 32]) -> Result<Option<String>> {
        self.get_mapping(MELT_QUOTE_IDS_TABLE, payment_hash)
    }

    fn insert_mapping(
        &self,
        definition: TableDefinition<&[u8; 32], &str>,
        payment_hash: &[u8; 32],
        value: &str,
    ) -> Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(definition)?;
            table.insert(payment_hash, value)?;
        }
        write_txn.commit()?;
        tracing::debug!(
            "Inserted lexe mapping: {} -> {}",
            hex::encode(payment_hash),
            value
        );
        Ok(())
    }

    fn get_mapping(
        &self,
        definition: TableDefinition<&[u8; 32], &str>,
        payment_hash: &[u8; 32],
    ) -> Result<Option<String>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(definition)?;
        let result = table.get(payment_hash)?;
        Ok(result.map(|value| value.value().to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::QuoteDatabase;

    fn test_db_path() -> std::path::PathBuf {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock before Unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "cdk-lexe-quotes-{}-{unique}.redb",
            std::process::id()
        ))
    }

    #[test]
    fn persists_all_quote_mappings() {
        let path = test_db_path();
        let hash = [42_u8; 32];

        {
            let db = QuoteDatabase::new(&path).expect("create quote database");
            db.insert_mint_quote(&hash, "lnbc1incoming-invoice")
                .expect("insert mint invoice");
            db.insert_mint_payment_id(&hash, "0001-ln_aabbcc")
                .expect("insert mint payment index");
            db.insert_melt_quote(&hash, "lnbc1outgoing-invoice")
                .expect("insert melt invoice");
            db.insert_melt_payment_id(&hash, "0002-ln_ddddeeff")
                .expect("insert melt payment index");
            db.insert_melt_quote_id(&hash, "quote-id-1", "msat")
                .expect("insert melt quote id");
        }

        let db = QuoteDatabase::new(&path).expect("reopen quote database");
        assert_eq!(
            db.get_mint_quote(&hash).expect("get mint invoice"),
            Some("lnbc1incoming-invoice".to_string())
        );
        assert_eq!(
            db.get_mint_payment_id(&hash)
                .expect("get mint payment index"),
            Some("0001-ln_aabbcc".to_string())
        );
        assert_eq!(
            db.get_melt_quote(&hash).expect("get melt invoice"),
            Some("lnbc1outgoing-invoice".to_string())
        );
        assert_eq!(
            db.get_melt_payment_id(&hash)
                .expect("get melt payment index"),
            Some("0002-ln_ddddeeff".to_string())
        );
        assert_eq!(
            db.get_melt_quote_id(&hash).expect("get melt quote id"),
            Some(r#"{"quote_id":"quote-id-1","unit":"msat"}"#.to_string())
        );

        // Unknown hashes return None.
        let other = [7_u8; 32];
        assert_eq!(db.get_mint_quote(&other).expect("miss"), None);
        assert_eq!(db.get_melt_payment_id(&other).expect("miss"), None);

        drop(db);
        std::fs::remove_file(path).expect("remove quote database");
    }

    #[test]
    fn upsert_overwrites_stale_values() {
        let path = test_db_path();
        let hash = [9_u8; 32];

        let db = QuoteDatabase::new(&path).expect("create quote database");
        db.insert_melt_payment_id(&hash, "0001-ln_old").unwrap();
        db.insert_melt_payment_id(&hash, "0002-ln_new").unwrap();

        assert_eq!(
            db.get_melt_payment_id(&hash).expect("get"),
            Some("0002-ln_new".to_string())
        );

        drop(db);
        std::fs::remove_file(path).expect("remove quote database");
    }
}
