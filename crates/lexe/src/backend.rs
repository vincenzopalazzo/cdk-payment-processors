//! Lexe backend for the CDK payment processor.
//!
//! Talks to a Lexe managed non-custodial Lightning node through the Lexe
//! Rust SDK. BOLT11 only. Retry safety comes from a local redb database
//! keyed by BOLT11 payment hash, since the Lexe node does not accept a
//! client idempotency token for payments.

use std::collections::HashMap;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use anyhow::{anyhow, Context as _};
use async_trait::async_trait;
use cdk_common::bitcoin::hashes::Hash as _;
use cdk_common::nuts::{CurrencyUnit, MeltQuoteState};
use cdk_common::payment::{
    Bolt11Settings, CreateIncomingPaymentResponse, Error, Event, IncomingPaymentOptions,
    MakePaymentResponse, MintPayment, OutgoingPaymentOptions, PaymentIdentifier,
    PaymentQuoteResponse, SettingsResponse, WaitPaymentResponse,
};
use cdk_common::util::unix_time;
use cdk_common::{Amount, Bolt11Invoice, QuoteId};
use futures::Stream;
use lexe::config::WalletEnvConfig;
use lexe::types::auth::{ClientCredentials, CredentialsRef, RootSeed};
use lexe::types::bitcoin::{Amount as LexeAmount, Invoice as LexeInvoice};
use lexe::types::command::{
    CreateInvoiceRequest, GetPaymentRequest, PayInvoiceRequest, WaitForNextPaymentRequest,
};
use lexe::types::payment::{
    Payment, PaymentCreatedIndex, PaymentDirection, PaymentFilter, PaymentStatus,
    PaymentUpdatedIndex,
};
use lexe::util::ByteArray;
use lexe::wallet::LexeWallet;
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;

use crate::database::QuoteDatabase;
use crate::settings::Config;

/// How long the payment-update poller waits for the next Lexe update.
const POLL_TIMEOUT: Duration = Duration::from_secs(30);
/// Initial backoff after a failed poll.
const POLL_BACKOFF_INITIAL: Duration = Duration::from_millis(500);
/// Maximum backoff after a failed poll.
const POLL_BACKOFF_MAX: Duration = Duration::from_secs(30);
/// How many recent Lexe payments to scan when recovering an in-flight
/// outgoing payment whose index was not stored yet.
const RECOVERY_SCAN_LIMIT: usize = 500;

/// Event stream that releases its Lexe poller activity on completion or drop.
struct PaymentEventStream {
    receiver: ReceiverStream<Event>,
    activity: Arc<PaymentEventStreamActivity>,
}

struct PaymentEventStreamActivity {
    active_streams: Arc<AtomicUsize>,
    active: AtomicBool,
}

impl PaymentEventStreamActivity {
    fn new(active_streams: Arc<AtomicUsize>) -> Self {
        active_streams.fetch_add(1, Ordering::Relaxed);
        Self {
            active_streams,
            active: AtomicBool::new(true),
        }
    }

    fn deactivate(&self) {
        if self.active.swap(false, Ordering::Relaxed) {
            self.active_streams.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

impl Drop for PaymentEventStreamActivity {
    fn drop(&mut self) {
        self.deactivate();
    }
}

impl PaymentEventStream {
    fn new(receiver: mpsc::Receiver<Event>, active_streams: Arc<AtomicUsize>) -> Self {
        Self {
            receiver: ReceiverStream::new(receiver),
            activity: Arc::new(PaymentEventStreamActivity::new(active_streams)),
        }
    }
}

impl Stream for PaymentEventStream {
    type Item = Event;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.get_mut().receiver).poll_next(cx)
    }
}

impl Drop for PaymentEventStream {
    fn drop(&mut self) {
        self.activity.deactivate();
    }
}

/// CDK payment backend backed by a Lexe managed Lightning node.
pub struct LexeBackend {
    wallet: Arc<LexeWallet>,
    db: QuoteDatabase,
    active_payment_streams: Arc<AtomicUsize>,
    event_cancel: watch::Sender<()>,
    fee_reserve_ppm: u32,
    fee_reserve_min_sat: u64,
    payment_timeout: Duration,
}

impl LexeBackend {
    /// Connect to the Lexe node from configuration.
    pub async fn new(config: &Config) -> Result<Self, Error> {
        let backend = &config.lexe;
        let data_dir = std::path::PathBuf::from(&backend.data_dir);
        std::fs::create_dir_all(&data_dir).map_err(|e| {
            Error::Custom(format!(
                "failed to create data dir {}: {e}",
                data_dir.display()
            ))
        })?;

        let env_config =
            wallet_env_config(&backend.network).map_err(|e| Error::Custom(e.to_string()))?;

        let wallet = if let Some(raw) = backend
            .client_credentials
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            let cc = decode_client_credentials(raw).map_err(|e| Error::Custom(e.to_string()))?;
            let credentials = CredentialsRef::ClientCredentials(&cc);
            let wallet =
                match LexeWallet::load(env_config.clone(), credentials, Some(data_dir.clone()))
                    .map_err(|e| Error::Custom(format!("failed to load Lexe wallet: {e}")))?
                {
                    Some(wallet) => wallet,
                    None => {
                        LexeWallet::fresh(env_config.clone(), credentials, Some(data_dir.clone()))
                            .map_err(|e| {
                            Error::Custom(format!("failed to initialize Lexe wallet: {e}"))
                        })?
                    }
                };
            wallet
                .provision(credentials)
                .await
                .map_err(|e| Error::Custom(format!("failed to provision Lexe node: {e}")))?;
            wallet
        } else if let Some(seed) = backend.seed_phrase.as_deref().map(str::trim) {
            let mnemonic = lexe::bip39::Mnemonic::parse_normalized(seed)
                .map_err(|e| Error::Custom(format!("invalid BIP39 mnemonic: {e}")))?;
            let root_seed =
                RootSeed::from_mnemonic(mnemonic).map_err(|e| Error::Custom(e.to_string()))?;
            let credentials = CredentialsRef::RootSeed(&root_seed);
            let wallet =
                match LexeWallet::load(env_config.clone(), credentials, Some(data_dir.clone()))
                    .map_err(|e| Error::Custom(format!("failed to load Lexe wallet: {e}")))?
                {
                    Some(wallet) => wallet,
                    None => {
                        let wallet = LexeWallet::fresh(
                            env_config.clone(),
                            credentials,
                            Some(data_dir.clone()),
                        )
                        .map_err(|e| {
                            Error::Custom(format!("failed to initialize Lexe wallet: {e}"))
                        })?;
                        match wallet.signup(&root_seed, None).await {
                            Ok(()) => tracing::info!("Signed up new Lexe node from seed phrase"),
                            Err(e) => tracing::warn!(
                                error = %e,
                                "Lexe signup failed; continuing (node may already be signed up)"
                            ),
                        }
                        wallet
                    }
                };
            wallet
                .provision(credentials)
                .await
                .map_err(|e| Error::Custom(format!("failed to provision Lexe node: {e}")))?;
            wallet
        } else {
            return Err(Error::Custom(
                "lexe credentials missing: set client_credentials or seed_phrase".into(),
            ));
        };

        let db = QuoteDatabase::new(data_dir.join("quotes.db"))
            .map_err(|e| Error::Custom(format!("failed to open quote database: {e}")))?;

        Ok(Self {
            wallet: Arc::new(wallet),
            db,
            active_payment_streams: Arc::new(AtomicUsize::new(0)),
            event_cancel: watch::channel(()).0,
            fee_reserve_ppm: backend.fee_reserve_ppm,
            fee_reserve_min_sat: u64::from(backend.fee_reserve_min_sat),
            payment_timeout: Duration::from_secs(backend.payment_timeout_secs),
        })
    }

    /// Best-effort lookup of a payment by BOLT11 payment hash in the recent
    /// Lexe payment history.
    async fn find_payment_by_hash(&self, hash: &[u8; 32]) -> Result<Option<Payment>, Error> {
        self.wallet
            .sync_payments()
            .await
            .map_err(|e| Error::Backend(anyhow!("Lexe payment sync failed: {e}").into()))?;
        let response = self
            .wallet
            .list_payments(&PaymentFilter::All, None, Some(RECOVERY_SCAN_LIMIT), None)
            .map_err(|e| Error::Backend(anyhow!("Lexe payment list failed: {e}").into()))?;
        Ok(response
            .payments
            .into_iter()
            .find(|p| p.hash.as_ref().map(|h| h.to_array()) == Some(*hash)))
    }

    /// Fetch a payment by stored Lexe index string.
    async fn fetch_payment(&self, index_str: &str) -> Result<Option<Payment>, Error> {
        let index: PaymentCreatedIndex = index_str.parse().map_err(|e| {
            Error::Custom(format!(
                "invalid stored Lexe payment index `{index_str}`: {e}"
            ))
        })?;
        let response = self
            .wallet
            .get_payment(GetPaymentRequest { index })
            .await
            .map_err(|e| Error::Backend(anyhow!("Lexe get_payment failed: {e}").into()))?;
        Ok(response.payment)
    }

    fn payment_hash_of(identifier: &PaymentIdentifier) -> Result<&[u8; 32], Error> {
        match identifier {
            PaymentIdentifier::PaymentHash(hash) => Ok(hash),
            _ => Err(Error::UnsupportedPaymentOption),
        }
    }

    fn invoice_amount_sats(invoice: &Bolt11Invoice) -> Result<u64, Error> {
        let amount_msat = invoice
            .amount_milli_satoshis()
            .ok_or(Error::AmountMismatch)?;
        let amount_sats = amount_msat.div_ceil(1000);
        if amount_sats == 0 {
            return Err(Error::AmountMismatch);
        }
        Ok(amount_sats)
    }

    /// Estimated outgoing fee: `ppm` of the amount, at least `min_sat`.
    fn estimate_fee_sats(amount_sats: u64, ppm: u32, min_sat: u64) -> u64 {
        let estimated = amount_sats
            .saturating_mul(u64::from(ppm))
            .saturating_add(999_999)
            / 1_000_000;
        estimated.max(min_sat)
    }

    fn melt_status(status: PaymentStatus) -> MeltQuoteState {
        match status {
            PaymentStatus::Completed => MeltQuoteState::Paid,
            PaymentStatus::Failed => MeltQuoteState::Failed,
            PaymentStatus::Pending => MeltQuoteState::Pending,
        }
    }

    fn outgoing_response(
        payment_identifier: PaymentIdentifier,
        unit: &CurrencyUnit,
        payment: &Payment,
    ) -> MakePaymentResponse {
        let status = Self::melt_status(payment.status);
        let total_spent = if status == MeltQuoteState::Paid {
            let sats = payment
                .amount
                .map(|a| a.sats_u64())
                .unwrap_or_default()
                .saturating_add(payment.fees.sats_u64());
            Amount::new(sats, CurrencyUnit::Sat)
                .convert_to(unit)
                .unwrap_or_else(|_| Amount::new(0, unit.clone()))
        } else {
            Amount::new(0, unit.clone())
        };
        MakePaymentResponse {
            payment_lookup_id: payment_identifier,
            payment_proof: payment
                .preimage
                .map(|preimage| hex::encode(preimage.to_array())),
            status,
            total_spent,
        }
    }

    /// Unit used by the melt quote for this payment hash (for event details).
    fn stored_melt_unit(&self, hash: &[u8; 32]) -> CurrencyUnit {
        self.db
            .get_melt_quote_id(hash)
            .ok()
            .flatten()
            .and_then(|raw| {
                serde_json::from_str::<serde_json::Value>(&raw)
                    .ok()
                    .and_then(|v| v.get("unit")?.as_str().map(str::to_string))
            })
            .and_then(|u| CurrencyUnit::from_str(&u).ok())
            .unwrap_or(CurrencyUnit::Sat)
    }
}

#[async_trait]
impl MintPayment for LexeBackend {
    type Err = Error;

    async fn start(&self) -> Result<(), Self::Err> {
        let node = self
            .wallet
            .node_info()
            .await
            .map_err(|e| Error::Backend(anyhow!("Lexe node_info failed: {e}").into()))?;
        tracing::info!(
            node_version = %node.version,
            lightning_balance_sats = node.lightning_balance.sats_u64(),
            "Lexe node reachable"
        );
        Ok(())
    }

    async fn get_settings(&self) -> Result<SettingsResponse, Self::Err> {
        Ok(SettingsResponse {
            unit: "sat".to_string(),
            bolt11: Some(Bolt11Settings {
                mpp: false,
                amountless: false,
                invoice_description: true,
            }),
            bolt12: None,
            onchain: None,
            custom: HashMap::new(),
        })
    }

    async fn create_incoming_payment_request(
        &self,
        options: IncomingPaymentOptions,
    ) -> Result<CreateIncomingPaymentResponse, Self::Err> {
        let IncomingPaymentOptions::Bolt11(opts) = options else {
            return Err(Error::UnsupportedPaymentOption);
        };

        let amount_sats = opts.amount.to_sat()?;
        if amount_sats == 0 {
            return Err(Error::AmountMismatch);
        }
        let description = opts.description.unwrap_or_default();
        let expiration_secs = match opts.unix_expiry {
            Some(expiry) => {
                let now = unix_time();
                if expiry <= now {
                    return Err(Error::InvalidExpiry);
                }
                Some(u32::try_from(expiry - now).map_err(|_| Error::InvalidExpiry)?)
            }
            None => None,
        };

        let lexe_amount = LexeAmount::try_from_sats_u64(amount_sats)
            .map_err(|e| Error::Custom(format!("invalid invoice amount: {e}")))?;

        let response = self
            .wallet
            .create_invoice(CreateInvoiceRequest {
                expiration_secs,
                amount: Some(lexe_amount),
                description: Some(description),
                personal_note: None,
                partner_pk: None,
                partner_prop_fee: None,
                partner_base_fee: None,
            })
            .await
            .map_err(|e| Error::Backend(anyhow!("Lexe create_invoice failed: {e}").into()))?;

        let invoice = response.invoice.to_string();
        let parsed = Bolt11Invoice::from_str(&invoice)?;
        let payment_hash = parsed.payment_hash().to_byte_array();

        self.db
            .insert_mint_quote(&payment_hash, &invoice)
            .map_err(|e| Error::Custom(e.to_string()))?;
        self.db
            .insert_mint_payment_id(&payment_hash, &response.index.to_string())
            .map_err(|e| Error::Custom(e.to_string()))?;

        Ok(CreateIncomingPaymentResponse {
            request_lookup_id: PaymentIdentifier::PaymentHash(payment_hash),
            request: invoice,
            expiry: opts.unix_expiry,
            extra_json: None,
        })
    }

    async fn get_payment_quote(
        &self,
        unit: &CurrencyUnit,
        options: OutgoingPaymentOptions,
    ) -> Result<PaymentQuoteResponse, Self::Err> {
        let OutgoingPaymentOptions::Bolt11(opts) = options else {
            return Err(Error::UnsupportedPaymentOption);
        };

        let invoice = opts.bolt11.to_string();
        let amount_sats = Self::invoice_amount_sats(&opts.bolt11)?;
        let fee_sats =
            Self::estimate_fee_sats(amount_sats, self.fee_reserve_ppm, self.fee_reserve_min_sat);
        let payment_hash = opts.bolt11.payment_hash().to_byte_array();

        self.db
            .insert_melt_quote(&payment_hash, &invoice)
            .map_err(|e| Error::Custom(e.to_string()))?;
        self.db
            .insert_melt_quote_id(&payment_hash, &opts.quote_id.to_string(), &unit.to_string())
            .map_err(|e| Error::Custom(e.to_string()))?;

        Ok(PaymentQuoteResponse {
            request_lookup_id: Some(PaymentIdentifier::PaymentHash(payment_hash)),
            amount: Amount::from(amount_sats).with_unit(unit.clone()),
            fee: Amount::from(fee_sats).with_unit(unit.clone()),
            state: MeltQuoteState::Unpaid,
            extra_json: None,
            estimated_blocks: None,
            fee_options: None,
        })
    }

    async fn make_payment(
        &self,
        unit: &CurrencyUnit,
        options: OutgoingPaymentOptions,
    ) -> Result<MakePaymentResponse, Self::Err> {
        let OutgoingPaymentOptions::Bolt11(opts) = options else {
            return Err(Error::UnsupportedPaymentOption);
        };

        let invoice = opts.bolt11.to_string();
        let amount_sats = Self::invoice_amount_sats(&opts.bolt11)?;
        let payment_hash = opts.bolt11.payment_hash().to_byte_array();
        let identifier = PaymentIdentifier::PaymentHash(payment_hash);

        self.db
            .insert_melt_quote(&payment_hash, &invoice)
            .map_err(|e| Error::Custom(e.to_string()))?;
        self.db
            .insert_melt_quote_id(&payment_hash, &opts.quote_id.to_string(), &unit.to_string())
            .map_err(|e| Error::Custom(e.to_string()))?;

        // Idempotency: if we already have a Lexe payment for this invoice,
        // report its current state instead of paying again.
        if let Some(index) = self
            .db
            .get_melt_payment_id(&payment_hash)
            .map_err(|e| Error::Custom(e.to_string()))?
        {
            if let Some(payment) = self.fetch_payment(&index).await? {
                return Ok(Self::outgoing_response(identifier, unit, &payment));
            }
        }

        let lexe_invoice = LexeInvoice::from_str(&invoice).map_err(|e| {
            Error::Custom(format!("failed to parse outgoing invoice for Lexe: {e}"))
        })?;
        // Lexe rejects a fallback amount that differs from the invoice amount,
        // and only needs one for amountless invoices. Invoices without an
        // amount are already rejected above (invoice_amount_sats), so this is
        // only a defensive value for the amountless case.
        let fallback_amount = if opts.bolt11.amount_milli_satoshis().is_some() {
            None
        } else {
            Some(
                LexeAmount::try_from_sats_u64(amount_sats)
                    .map_err(|e| Error::Custom(format!("invalid invoice amount: {e}")))?,
            )
        };
        let timeout = opts
            .timeout_secs
            .map(Duration::from_secs)
            .unwrap_or(self.payment_timeout);

        match tokio::time::timeout(
            timeout,
            self.wallet.pay_invoice(PayInvoiceRequest {
                invoice: lexe_invoice,
                fallback_amount,
                personal_note: None,
            }),
        )
        .await
        {
            Ok(Ok(payment)) => {
                self.db
                    .insert_melt_payment_id(&payment_hash, &payment.index.to_string())
                    .map_err(|e| Error::Custom(e.to_string()))?;
                Ok(Self::outgoing_response(identifier, unit, &payment))
            }
            Ok(Err(e)) => Err(Error::Backend(
                anyhow!("Lexe pay_invoice failed: {e}").into(),
            )),
            Err(_) => {
                tracing::warn!(
                    payment_hash = %hex::encode(payment_hash),
                    ?timeout,
                    "Lexe pay_invoice timed out; payment may still be in flight"
                );
                match self.find_payment_by_hash(&payment_hash).await? {
                    Some(payment) => {
                        self.db
                            .insert_melt_payment_id(&payment_hash, &payment.index.to_string())
                            .map_err(|e| Error::Custom(e.to_string()))?;
                        Ok(Self::outgoing_response(identifier, unit, &payment))
                    }
                    None => Ok(MakePaymentResponse {
                        payment_lookup_id: identifier,
                        payment_proof: None,
                        status: MeltQuoteState::Pending,
                        total_spent: Amount::new(0, unit.clone()),
                    }),
                }
            }
        }
    }

    async fn wait_payment_event(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = Event> + Send>>, Self::Err> {
        let (sender, receiver) = mpsc::channel(100);
        let stream = PaymentEventStream::new(receiver, Arc::clone(&self.active_payment_streams));
        let activity = Arc::clone(&stream.activity);
        let mut cancel = self.event_cancel.subscribe();
        let wallet = Arc::clone(&self.wallet);
        let db = self.db.clone();

        tokio::spawn(async move {
            let mut backoff = POLL_BACKOFF_INITIAL;
            // SDK-documented cursor: each poll starts after the previous
            // update, so an event is never delivered twice.
            let mut cursor: Option<PaymentUpdatedIndex> = None;
            loop {
                tokio::select! {
                    _ = cancel.changed() => break,
                    _ = sender.closed() => break,
                    result = wallet.wait_for_next_payment(WaitForNextPaymentRequest {
                        start_index: cursor,
                        timeout: Some(POLL_TIMEOUT),
                    }) => {
                        match result {
                            Ok(response) => {
                                cursor = Some(response.next_start_index);
                                backoff = POLL_BACKOFF_INITIAL;
                                if let Some(event) =
                                    map_payment_event(&db, &response.payment)
                                {
                                    if sender.send(event).await.is_err() {
                                        break;
                                    }
                                }
                            }
                            Err(err) => {
                                tracing::debug!("Lexe payment update wait: {err}");
                                tokio::select! {
                                    _ = tokio::time::sleep(backoff) => {}
                                    _ = cancel.changed() => break,
                                }
                                backoff = (backoff * 2).min(POLL_BACKOFF_MAX);
                            }
                        }
                    }
                }
            }
            activity.deactivate();
        });

        Ok(Box::pin(stream))
    }

    fn is_payment_event_stream_active(&self) -> bool {
        self.active_payment_streams.load(Ordering::Relaxed) > 0
    }

    fn cancel_payment_event_stream(&self) {
        let _ = self.event_cancel.send(());
    }

    async fn check_incoming_payment_status(
        &self,
        payment_identifier: &PaymentIdentifier,
    ) -> Result<Vec<WaitPaymentResponse>, Self::Err> {
        let payment_hash = Self::payment_hash_of(payment_identifier)?;
        let Some(index) = self
            .db
            .get_mint_payment_id(payment_hash)
            .map_err(|e| Error::Custom(e.to_string()))?
        else {
            return Ok(vec![]);
        };
        let Some(payment) = self.fetch_payment(&index).await? else {
            return Ok(vec![]);
        };
        if payment.status != PaymentStatus::Completed {
            return Ok(vec![]);
        }

        let amount_sats = payment.amount.map(|a| a.sats_u64()).unwrap_or_else(|| {
            self.db
                .get_mint_quote(payment_hash)
                .ok()
                .flatten()
                .and_then(|invoice| Bolt11Invoice::from_str(&invoice).ok())
                .and_then(|invoice| invoice.amount_milli_satoshis())
                .map(|msat| msat.div_ceil(1000))
                .unwrap_or(0)
        });

        Ok(vec![WaitPaymentResponse {
            payment_id: payment.index.to_string(),
            payment_identifier: payment_identifier.clone(),
            payment_amount: Amount::new(amount_sats, CurrencyUnit::Sat),
        }])
    }

    async fn check_outgoing_payment(
        &self,
        payment_identifier: &PaymentIdentifier,
    ) -> Result<MakePaymentResponse, Self::Err> {
        let payment_hash = *Self::payment_hash_of(payment_identifier)?;

        if let Some(index) = self
            .db
            .get_melt_payment_id(&payment_hash)
            .map_err(|e| Error::Custom(e.to_string()))?
        {
            if let Some(payment) = self.fetch_payment(&index).await? {
                let unit = self.stored_melt_unit(&payment_hash);
                return Ok(Self::outgoing_response(
                    payment_identifier.clone(),
                    &unit,
                    &payment,
                ));
            }
            // Index stored but no payment record yet: still in flight.
            return Ok(MakePaymentResponse {
                payment_lookup_id: payment_identifier.clone(),
                payment_proof: None,
                status: MeltQuoteState::Pending,
                total_spent: Amount::new(0, CurrencyUnit::Sat),
            });
        }

        // No stored index: was this invoice ever quoted?
        if self
            .db
            .get_melt_quote(&payment_hash)
            .map_err(|e| Error::Custom(e.to_string()))?
            .is_none()
        {
            return Err(Error::Custom("Outgoing payment not found".to_string()));
        }

        // Quoted but no Lexe payment index stored yet (e.g. make_payment
        // timed out before the index was recorded). Scan recent payments.
        if let Some(payment) = self.find_payment_by_hash(&payment_hash).await? {
            self.db
                .insert_melt_payment_id(&payment_hash, &payment.index.to_string())
                .map_err(|e| Error::Custom(e.to_string()))?;
            let unit = self.stored_melt_unit(&payment_hash);
            return Ok(Self::outgoing_response(
                payment_identifier.clone(),
                &unit,
                &payment,
            ));
        }

        // Conservatively Pending: the invoice may still settle remotely.
        Ok(MakePaymentResponse {
            payment_lookup_id: payment_identifier.clone(),
            payment_proof: None,
            status: MeltQuoteState::Pending,
            total_spent: Amount::new(0, CurrencyUnit::Sat),
        })
    }
}

/// Decode a base64 Lexe SDK client-credentials blob.
fn decode_client_credentials(raw: &str) -> anyhow::Result<ClientCredentials> {
    use base64::Engine as _;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(raw.trim())
        .context("client credentials are not valid base64")?;
    let credentials: ClientCredentials = serde_json::from_slice(&bytes)
        .context("client credentials are not a valid SDK credentials blob")?;
    Ok(credentials)
}

/// Wallet environment for the configured network.
fn wallet_env_config(network: &str) -> std::result::Result<WalletEnvConfig, anyhow::Error> {
    let network = network.to_ascii_lowercase();
    let config = match network.as_str() {
        "mainnet" => WalletEnvConfig::mainnet(),
        "testnet3" | "testnet" => WalletEnvConfig::testnet3(),
        "regtest" => WalletEnvConfig::regtest(true, None::<String>),
        other => anyhow::bail!("invalid network `{other}`"),
    };
    Ok(config)
}

/// Map a Lexe payment update to a CDK event, if it is one of our payments.
fn map_payment_event(db: &QuoteDatabase, payment: &Payment) -> Option<Event> {
    let hash = payment.hash.as_ref()?.to_array();

    match payment.direction {
        PaymentDirection::Inbound => {
            if payment.status != PaymentStatus::Completed {
                return None;
            }
            // Only emit for invoices we created.
            db.get_mint_quote(&hash).ok().flatten()?;
            let amount_sats = payment.amount.map(|a| a.sats_u64()).unwrap_or_default();
            Some(Event::PaymentReceived(WaitPaymentResponse {
                payment_id: payment.index.to_string(),
                payment_identifier: PaymentIdentifier::PaymentHash(hash),
                payment_amount: Amount::new(amount_sats, CurrencyUnit::Sat),
            }))
        }
        PaymentDirection::Outbound => {
            let raw = db.get_melt_quote_id(&hash).ok()??;
            let value = serde_json::from_str::<serde_json::Value>(&raw).ok()?;
            let quote_id: QuoteId = value.get("quote_id")?.as_str()?.parse().ok()?;
            match payment.status {
                PaymentStatus::Completed => {
                    let unit = value
                        .get("unit")
                        .and_then(|u| u.as_str())
                        .and_then(|u| CurrencyUnit::from_str(u).ok())
                        .unwrap_or(CurrencyUnit::Sat);
                    let sats = payment
                        .amount
                        .map(|a| a.sats_u64())
                        .unwrap_or_default()
                        .saturating_add(payment.fees.sats_u64());
                    let total_spent = Amount::new(sats, CurrencyUnit::Sat)
                        .convert_to(&unit)
                        .unwrap_or_else(|_| Amount::new(0, unit.clone()));
                    let details = MakePaymentResponse {
                        payment_lookup_id: PaymentIdentifier::PaymentHash(hash),
                        payment_proof: payment
                            .preimage
                            .map(|preimage| hex::encode(preimage.to_array())),
                        status: MeltQuoteState::Paid,
                        total_spent,
                    };
                    Some(Event::PaymentSuccessful { quote_id, details })
                }
                PaymentStatus::Failed => Some(Event::PaymentFailed {
                    quote_id,
                    reason: payment.status_msg.clone(),
                }),
                PaymentStatus::Pending => None,
            }
        }
        PaymentDirection::Info => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine as _;
    use lexe::types::payment::{PaymentKind, PaymentRail};
    use lexe::types::util::TimestampMs;

    #[test]
    fn fee_estimate_uses_ppm_and_minimum() {
        // 100 ppm of 1000 sats = 0.1 sats -> ceil to 1.
        assert_eq!(LexeBackend::estimate_fee_sats(1000, 100, 1), 1);
        // 100 ppm of 50000 sats = 5 sats.
        assert_eq!(LexeBackend::estimate_fee_sats(50000, 100, 1), 5);
        // Minimum applies below the computed value.
        assert_eq!(LexeBackend::estimate_fee_sats(10, 100, 1), 1);
        // 1000 ppm (0.1%) of 100 sats = 0.1 -> rounds up to 1.
        assert_eq!(LexeBackend::estimate_fee_sats(100, 1000, 1), 1);
        // 1000 ppm of 10000 sats = 10.
        assert_eq!(LexeBackend::estimate_fee_sats(10000, 1000, 1), 10);
        // Zero amount still costs at least the minimum.
        assert_eq!(LexeBackend::estimate_fee_sats(0, 100, 2), 2);
    }

    #[test]
    fn maps_lex_statuses_to_melt_states() {
        assert_eq!(
            LexeBackend::melt_status(PaymentStatus::Completed),
            MeltQuoteState::Paid
        );
        assert_eq!(
            LexeBackend::melt_status(PaymentStatus::Failed),
            MeltQuoteState::Failed
        );
        assert_eq!(
            LexeBackend::melt_status(PaymentStatus::Pending),
            MeltQuoteState::Pending
        );
    }

    #[test]
    fn payment_index_round_trips_through_string() {
        // Index strings are "<19-digit-timestamp>-ln_<64-hex>".
        let raw = "0002683862736062841-ln_3ddc5b1e2a9c8f0e1d2b4a6c8e0f1d3b5a7c9e1f2d4b6a8c0e2f4a6c8d0b2e4f";
        let index: PaymentCreatedIndex = raw.parse().expect("valid index string");
        assert_eq!(index.to_string(), raw);
    }

    #[test]
    fn malformed_payment_index_rejected() {
        assert!("not-an-index".parse::<PaymentCreatedIndex>().is_err());
        assert!("0002683862736062841-"
            .parse::<PaymentCreatedIndex>()
            .is_err());
    }

    #[test]
    fn client_credentials_reject_invalid_base64() {
        assert!(decode_client_credentials("!!!not-base64!!!").is_err());
    }

    #[test]
    fn client_credentials_reject_non_json_payload() {
        let b64 = base64::engine::general_purpose::STANDARD.encode("this is not json");
        assert!(decode_client_credentials(&b64).is_err());
    }

    fn test_event_db_path() -> std::path::PathBuf {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock before Unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "cdk-lexe-events-{}-{unique}.redb",
            std::process::id()
        ))
    }

    fn test_payment(
        direction: PaymentDirection,
        status: PaymentStatus,
        hash: [u8; 32],
        amount_sats: u64,
        fees_sats: u64,
        status_msg: &str,
    ) -> Payment {
        Payment {
            index: "0002683862736062841-ln_3ddc5b1e2a9c8f0e1d2b4a6c8e0f1d3b5a7c9e1f2d4b6a8c0e2f4a6c8d0b2e4f"
                .parse()
                .expect("valid index string"),
            rail: PaymentRail::Invoice,
            kind: PaymentKind::Invoice,
            direction,
            hash: Some(hex::encode(hash).parse().expect("valid hex payment hash")),
            preimage: Some(
                hex::encode([9_u8; 32])
                    .parse()
                    .expect("valid hex preimage"),
            ),
            offer_id: None,
            txid: None,
            amount: Some(
                LexeAmount::try_from_sats_u64(amount_sats).expect("valid amount"),
            ),
            fees: LexeAmount::try_from_sats_u64(fees_sats).expect("valid fees"),
            partner_pk: None,
            partner_prop_fee: None,
            partner_base_fee: None,
            status,
            status_msg: status_msg.to_string(),
            address: None,
            invoice: None,
            tx: None,
            payer_name: None,
            message: None,
            personal_note: None,
            priority: None,
            expires_at: None,
            finalized_at: None,
            created_at: TimestampMs::from_secs_u32(1_700_000_000),
            updated_at: TimestampMs::from_secs_u32(1_700_000_001),
        }
    }

    #[test]
    fn inbound_completed_payment_becomes_payment_received() {
        let path = test_event_db_path();
        let db = QuoteDatabase::new(&path).expect("create quote database");
        let hash = [7_u8; 32];
        db.insert_mint_quote(&hash, "lnbc1incoming-invoice")
            .expect("insert mint quote");

        let payment = test_payment(
            PaymentDirection::Inbound,
            PaymentStatus::Completed,
            hash,
            21_000,
            0,
            "received",
        );
        match map_payment_event(&db, &payment).expect("event for our completed inbound payment") {
            Event::PaymentReceived(response) => {
                assert_eq!(
                    response.payment_identifier,
                    PaymentIdentifier::PaymentHash(hash)
                );
                assert_eq!(
                    response.payment_amount.to_sat().expect("sat amount"),
                    21_000
                );
            }
            other => panic!("expected PaymentReceived, got {other:?}"),
        }
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn inbound_payments_we_did_not_create_or_pending_are_ignored() {
        let path = test_event_db_path();
        let db = QuoteDatabase::new(&path).expect("create quote database");
        let ours = [10_u8; 32];
        let foreign = [11_u8; 32];
        db.insert_mint_quote(&ours, "lnbc1incoming-invoice")
            .expect("insert mint quote");

        // Settled, but for an invoice this processor did not create.
        let foreign_payment = test_payment(
            PaymentDirection::Inbound,
            PaymentStatus::Completed,
            foreign,
            21_000,
            0,
            "received",
        );
        assert!(map_payment_event(&db, &foreign_payment).is_none());

        // Our invoice, but not settled yet.
        let pending = test_payment(
            PaymentDirection::Inbound,
            PaymentStatus::Pending,
            ours,
            21_000,
            0,
            "pending",
        );
        assert!(map_payment_event(&db, &pending).is_none());
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn outbound_completed_payment_becomes_payment_successful() {
        let path = test_event_db_path();
        let db = QuoteDatabase::new(&path).expect("create quote database");
        let hash = [8_u8; 32];
        let quote_id: QuoteId = "018f8b3e-8c1a-7d2e-9f4b-6a1c3e5d7f90"
            .parse()
            .expect("valid quote id");
        db.insert_melt_quote_id(&hash, &quote_id.to_string(), "msat")
            .expect("insert melt quote id");

        let payment = test_payment(
            PaymentDirection::Outbound,
            PaymentStatus::Completed,
            hash,
            5_000,
            3,
            "settled",
        );
        match map_payment_event(&db, &payment).expect("event for our settled outbound payment") {
            Event::PaymentSuccessful {
                quote_id: event_quote_id,
                details,
            } => {
                assert_eq!(event_quote_id, quote_id);
                assert_eq!(details.status, MeltQuoteState::Paid);
                // msat unit: 5003 sats -> 5_003_000 msats
                assert_eq!(
                    details.total_spent.to_msat().expect("msat amount"),
                    5_003_000
                );
                assert!(details.payment_proof.is_some());
            }
            other => panic!("expected PaymentSuccessful, got {other:?}"),
        }
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn outbound_failed_payment_becomes_payment_failed() {
        let path = test_event_db_path();
        let db = QuoteDatabase::new(&path).expect("create quote database");
        let hash = [9_u8; 32];
        let quote_id: QuoteId = "018f8b3e-8c1a-7d2e-9f4b-6a1c3e5d7f91"
            .parse()
            .expect("valid quote id");
        db.insert_melt_quote_id(&hash, &quote_id.to_string(), "sat")
            .expect("insert melt quote id");

        let payment = test_payment(
            PaymentDirection::Outbound,
            PaymentStatus::Failed,
            hash,
            5_000,
            0,
            "timed out",
        );
        match map_payment_event(&db, &payment).expect("event for our failed outbound payment") {
            Event::PaymentFailed {
                quote_id: event_quote_id,
                reason,
            } => {
                assert_eq!(event_quote_id, quote_id);
                assert_eq!(reason, "timed out");
            }
            other => panic!("expected PaymentFailed, got {other:?}"),
        }
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn outbound_pending_payment_is_ignored_until_terminal() {
        let path = test_event_db_path();
        let db = QuoteDatabase::new(&path).expect("create quote database");
        let hash = [12_u8; 32];
        let quote_id: QuoteId = "018f8b3e-8c1a-7d2e-9f4b-6a1c3e5d7f92"
            .parse()
            .expect("valid quote id");
        db.insert_melt_quote_id(&hash, &quote_id.to_string(), "sat")
            .expect("insert melt quote id");

        let payment = test_payment(
            PaymentDirection::Outbound,
            PaymentStatus::Pending,
            hash,
            5_000,
            0,
            "in flight",
        );
        assert!(map_payment_event(&db, &payment).is_none());
        std::fs::remove_file(&path).ok();
    }
}
