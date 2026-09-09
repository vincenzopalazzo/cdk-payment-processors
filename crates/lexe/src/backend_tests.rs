use super::{tests::test_payment, *};
use cdk_common::payment::{Bolt11IncomingPaymentOptions, Bolt11OutgoingPaymentOptions};
use cdk_common::QuoteId;
use futures::StreamExt;
use lexe::types::command::{
    CreateInvoiceResponse, GetPaymentResponse, GetUpdatedPaymentsResponse, ListPaymentsResponse,
    NodeInfo,
};
use lexe::types::util::TimestampMs;
use std::sync::Mutex;

#[derive(Default)]
struct MockClient {
    payments: Mutex<Vec<Payment>>,
    outcome: Mutex<Option<Payment>>,
    submissions: AtomicUsize,
    syncs: AtomicUsize,
    pages: AtomicUsize,
    update_pages: AtomicUsize,
    block_submission: AtomicBool,
    submitted: tokio::sync::Notify,
}

#[async_trait]
impl LexeClient for MockClient {
    async fn node_info(&self) -> anyhow::Result<NodeInfo> {
        anyhow::bail!("unexpected node_info")
    }

    async fn create_invoice(
        &self,
        _: CreateInvoiceRequest,
    ) -> anyhow::Result<CreateInvoiceResponse> {
        anyhow::bail!("unexpected create_invoice")
    }

    async fn pay_invoice(&self, _: PayInvoiceRequest) -> anyhow::Result<Payment> {
        self.submissions.fetch_add(1, Ordering::SeqCst);
        self.submitted.notify_one();
        if self.block_submission.load(Ordering::SeqCst) {
            return std::future::pending().await;
        }
        let payment = self
            .outcome
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| anyhow!("connection lost after submission"))?;
        self.payments.lock().unwrap().push(payment.clone());
        Ok(payment)
    }

    async fn sync_payments(&self) -> anyhow::Result<()> {
        self.syncs.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn list_payments(
        &self,
        limit: usize,
        after: Option<&PaymentCreatedIndex>,
    ) -> anyhow::Result<ListPaymentsResponse> {
        self.pages.fetch_add(1, Ordering::SeqCst);
        let mut payments = self.payments.lock().unwrap().clone();
        payments.sort_by_key(|p| std::cmp::Reverse(p.index));
        payments.retain(|p| after.is_none_or(|after| p.index < *after));
        let more = payments.len() > limit;
        payments.truncate(limit);
        let next_index = if more {
            payments.last().map(|p| p.index)
        } else {
            None
        };
        Ok(ListPaymentsResponse {
            payments,
            next_index,
        })
    }

    async fn get_payment(&self, req: GetPaymentRequest) -> anyhow::Result<GetPaymentResponse> {
        Ok(GetPaymentResponse {
            payment: self
                .payments
                .lock()
                .unwrap()
                .iter()
                .find(|p| p.index == req.index)
                .cloned(),
        })
    }

    async fn get_updated_payments(
        &self,
        req: GetUpdatedPaymentsRequest,
    ) -> anyhow::Result<GetUpdatedPaymentsResponse> {
        self.update_pages.fetch_add(1, Ordering::SeqCst);
        let mut payments = self.payments.lock().unwrap().clone();
        payments.sort_by_key(Payment::updated_index);
        payments.retain(|p| {
            req.start_index
                .is_none_or(|after| p.updated_index() > after)
        });
        payments.truncate(req.limit.unwrap_or(usize::MAX));
        Ok(GetUpdatedPaymentsResponse {
            updated_index: payments.last().map(Payment::updated_index),
            payments,
        })
    }
}

fn backend_at(client: Arc<MockClient>, path: &std::path::Path) -> LexeBackend {
    LexeBackend {
        wallet: client,
        db: QuoteDatabase::new(path.join("quotes.db")).unwrap(),
        active_payment_streams: Arc::new(AtomicUsize::new(0)),
        event_cancel: watch::channel(()).0,
        fee_reserve_ppm: 10_000,
        fee_reserve_min_sat: 2,
        payment_timeout: Duration::from_secs(10),
    }
}

fn fixture() -> (LexeBackend, Arc<MockClient>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let client = Arc::new(MockClient::default());
    (backend_at(client.clone(), dir.path()), client, dir)
}

fn options() -> Bolt11OutgoingPaymentOptions {
    Bolt11OutgoingPaymentOptions {
        bolt11: "lnbc100n1p5z3a63pp56854ytysg7e5z9fl3w5mgvrlqjfcytnjv8ff5hm5qt6gl6alxesqdqqcqzzsxqyz5vqsp5p0x0dlhn27s63j4emxnk26p7f94u0lyarnfp5yqmac9gzy4ngdss9qxpqysgqne3v0hnzt2lp0hc69xpzckk0cdcar7glvjhq60lsrfe8gejdm8c564prrnsft6ctxxyrewp4jtezrq3gxxqnfjj0f9tw2qs9y0lslmqpfu7et9".parse().unwrap(),
        max_fee_amount: None,
        timeout_secs: None,
        melt_options: None,
        quote_id: QuoteId::new(),
    }
}

fn outgoing(options: &Bolt11OutgoingPaymentOptions) -> OutgoingPaymentOptions {
    OutgoingPaymentOptions::Bolt11(Box::new(options.clone()))
}

fn identifier(options: &Bolt11OutgoingPaymentOptions) -> PaymentIdentifier {
    PaymentIdentifier::PaymentHash(options.bolt11.payment_hash().to_byte_array())
}

fn payment(n: u32, direction: PaymentDirection, hash: [u8; 32]) -> Payment {
    let mut payment = test_payment(direction, PaymentStatus::Completed, hash, 10, 0, "settled");
    payment.created_at = TimestampMs::from_secs_u32(1_700_000_000 + n);
    payment.updated_at = payment.created_at;
    payment.index = format!(
        "{:019}-ln_{}",
        1_700_000_000_000u64 + u64::from(n) * 1000,
        hex::encode(hash)
    )
    .parse()
    .unwrap();
    payment
}

#[tokio::test]
async fn unsupported_units_are_rejected_before_backend_calls() {
    let (backend, client, _dir) = fixture();
    for unit in [CurrencyUnit::Msat, CurrencyUnit::Usd] {
        assert!(matches!(
            backend.get_payment_quote(&unit, outgoing(&options())).await,
            Err(Error::UnsupportedUnit)
        ));
        assert!(matches!(
            backend.make_payment(&unit, outgoing(&options())).await,
            Err(Error::UnsupportedUnit)
        ));
        assert!(matches!(
            backend
                .create_incoming_payment_request(IncomingPaymentOptions::Bolt11(
                    Bolt11IncomingPaymentOptions {
                        amount: Amount::new(10_000, unit),
                        ..Default::default()
                    }
                ))
                .await,
            Err(Error::UnsupportedUnit)
        ));
    }
    assert_eq!(client.submissions.load(Ordering::SeqCst), 0);
    let quote = backend
        .get_payment_quote(&CurrencyUnit::Sat, outgoing(&options()))
        .await
        .unwrap();
    assert_eq!(quote.amount, Amount::new(10, CurrencyUnit::Sat));
    assert_eq!(quote.fee, Amount::new(2, CurrencyUnit::Sat));
}

#[tokio::test]
async fn capped_payments_are_rejected_without_creating_an_attempt() {
    let (backend, client, _dir) = fixture();
    let mut opts = options();
    for limit in [0, 1, 1000] {
        opts.max_fee_amount = Some(Amount::new(limit, CurrencyUnit::Sat));
        let error = backend
            .make_payment(&CurrencyUnit::Sat, outgoing(&opts))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("cannot enforce max_fee_amount"));
        let status = backend
            .check_outgoing_payment(&identifier(&opts))
            .await
            .unwrap();
        assert_eq!(status.status, MeltQuoteState::Unpaid);
    }
    assert_eq!(client.submissions.load(Ordering::SeqCst), 0);
    assert_eq!(client.syncs.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn quoting_again_does_not_reassign_the_attempt_or_event() {
    let (backend, client, _dir) = fixture();
    let opts = options();
    let hash = opts.bolt11.payment_hash().to_byte_array();
    let paid = payment(0, PaymentDirection::Outbound, hash);
    *client.outcome.lock().unwrap() = Some(paid.clone());
    backend
        .make_payment(&CurrencyUnit::Sat, outgoing(&opts))
        .await
        .unwrap();
    let next = options();
    backend
        .get_payment_quote(&CurrencyUnit::Sat, outgoing(&next))
        .await
        .unwrap();
    backend
        .make_payment(&CurrencyUnit::Sat, outgoing(&next))
        .await
        .unwrap();
    let Event::PaymentSuccessful { quote_id, .. } =
        map_payment_event(&backend.db, &paid).unwrap().unwrap()
    else {
        panic!("missing success")
    };
    assert_eq!(quote_id, opts.quote_id);
    assert_eq!(client.submissions.load(Ordering::SeqCst), 1);
}

#[test]
fn fractional_principal_and_fee_are_summed_before_rounding() {
    let mut paid = payment(0, PaymentDirection::Outbound, [1; 32]);
    for (principal, fee, sats) in [(10_000, 500, 11), (10_500, 500, 11), (10_500, 600, 12)] {
        paid.amount = Some(LexeAmount::from_msat(principal));
        paid.fees = LexeAmount::from_msat(fee);
        let result = LexeBackend::outgoing_response(
            PaymentIdentifier::PaymentHash([1; 32]),
            &CurrencyUnit::Sat,
            &paid,
        )
        .unwrap();
        assert_eq!(result.total_spent, Amount::new(sats, CurrencyUnit::Sat));
        let legacy = LexeBackend::outgoing_response(
            PaymentIdentifier::PaymentHash([1; 32]),
            &CurrencyUnit::Msat,
            &paid,
        )
        .unwrap();
        assert_eq!(
            legacy.total_spent,
            Amount::new(principal + fee, CurrencyUnit::Msat)
        );
    }
}

#[tokio::test]
async fn prepared_quotes_remain_unpaid_across_restart() {
    let (backend, client, dir) = fixture();
    let quote = backend
        .get_payment_quote(&CurrencyUnit::Sat, outgoing(&options()))
        .await
        .unwrap();
    drop(backend);
    let backend = backend_at(client.clone(), dir.path());
    assert_eq!(
        backend
            .check_outgoing_payment(&quote.request_lookup_id.unwrap())
            .await
            .unwrap()
            .status,
        MeltQuoteState::Unpaid
    );
    assert_eq!(client.syncs.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn timeout_is_recovered_after_restart_without_resubmission() {
    let (mut backend, client, dir) = fixture();
    backend.payment_timeout = Duration::from_millis(1);
    client.block_submission.store(true, Ordering::SeqCst);
    let opts = options();
    assert_eq!(
        backend
            .make_payment(&CurrencyUnit::Sat, outgoing(&opts))
            .await
            .unwrap()
            .status,
        MeltQuoteState::Pending
    );
    drop(backend);
    let backend = backend_at(client.clone(), dir.path());
    assert_eq!(
        backend
            .make_payment(&CurrencyUnit::Sat, outgoing(&opts))
            .await
            .unwrap()
            .status,
        MeltQuoteState::Pending
    );
    client.payments.lock().unwrap().push(payment(
        0,
        PaymentDirection::Outbound,
        opts.bolt11.payment_hash().to_byte_array(),
    ));
    assert_eq!(
        backend
            .make_payment(&CurrencyUnit::Sat, outgoing(&opts))
            .await
            .unwrap()
            .status,
        MeltQuoteState::Paid
    );
    assert_eq!(client.submissions.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn ambiguous_sdk_error_remains_pending_but_terminal_failure_is_recorded() {
    let (backend, client, _dir) = fixture();
    let opts = options();
    assert_eq!(
        backend
            .make_payment(&CurrencyUnit::Sat, outgoing(&opts))
            .await
            .unwrap()
            .status,
        MeltQuoteState::Pending
    );
    assert_eq!(
        backend
            .make_payment(&CurrencyUnit::Sat, outgoing(&opts))
            .await
            .unwrap()
            .status,
        MeltQuoteState::Pending
    );
    let mut failed = payment(
        0,
        PaymentDirection::Outbound,
        opts.bolt11.payment_hash().to_byte_array(),
    );
    failed.status = PaymentStatus::Failed;
    client.payments.lock().unwrap().push(failed);
    assert_eq!(
        backend
            .check_outgoing_payment(&identifier(&opts))
            .await
            .unwrap()
            .status,
        MeltQuoteState::Failed
    );
    assert_eq!(
        backend
            .make_payment(&CurrencyUnit::Sat, outgoing(&opts))
            .await
            .unwrap()
            .status,
        MeltQuoteState::Failed
    );
    assert_eq!(client.submissions.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn concurrent_retry_and_cancelled_rpc_do_not_submit_twice() {
    let (backend, client, _dir) = fixture();
    let backend = Arc::new(backend);
    client.block_submission.store(true, Ordering::SeqCst);
    let opts = options();
    let first = {
        let backend = backend.clone();
        let opts = opts.clone();
        tokio::spawn(async move {
            backend
                .make_payment(&CurrencyUnit::Sat, outgoing(&opts))
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(2), client.submitted.notified())
        .await
        .unwrap();
    assert_eq!(
        backend
            .make_payment(&CurrencyUnit::Sat, outgoing(&opts))
            .await
            .unwrap()
            .status,
        MeltQuoteState::Pending
    );
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    assert_eq!(
        backend
            .make_payment(&CurrencyUnit::Sat, outgoing(&opts))
            .await
            .unwrap()
            .status,
        MeltQuoteState::Pending
    );
    assert_eq!(client.submissions.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn recovery_searches_past_500_records_and_ignores_inbound_matches() {
    let (backend, client, _dir) = fixture();
    let opts = options();
    let hash = opts.bolt11.payment_hash().to_byte_array();
    backend
        .db
        .begin_melt_attempt(&hash, &opts.quote_id, &CurrencyUnit::Sat)
        .unwrap();
    let paid = payment(0, PaymentDirection::Outbound, hash);
    let mut history = vec![paid.clone(), payment(600, PaymentDirection::Inbound, hash)];
    for n in 1..=550u32 {
        let mut other = [0; 32];
        other[..4].copy_from_slice(&n.to_be_bytes());
        history.push(payment(n, PaymentDirection::Outbound, other));
    }
    *client.payments.lock().unwrap() = history;
    assert_eq!(
        backend
            .check_outgoing_payment(&identifier(&opts))
            .await
            .unwrap()
            .status,
        MeltQuoteState::Paid
    );
    assert_eq!(
        backend.db.get_melt_payment_id(&hash).unwrap(),
        Some(paid.index.to_string())
    );
    assert_eq!(client.pages.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn reconnect_replays_cached_payments_including_the_unconsumed_batch() {
    let (backend, client, _dir) = fixture();
    for n in 1..=3u8 {
        backend.db.insert_mint_quote(&[n; 32], "invoice").unwrap();
        client.payments.lock().unwrap().push(payment(
            u32::from(n),
            PaymentDirection::Inbound,
            [n; 32],
        ));
    }
    let mut first = backend.wait_payment_event().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), first.next())
        .await
        .unwrap()
        .unwrap();
    drop(first);
    let mut second = backend.wait_payment_event().await.unwrap();
    for n in 1..=3u8 {
        let event = tokio::time::timeout(Duration::from_secs(2), second.next())
            .await
            .unwrap()
            .unwrap();
        let Event::PaymentReceived(response) = event else {
            panic!("expected incoming")
        };
        assert_eq!(
            response.payment_identifier,
            PaymentIdentifier::PaymentHash([n; 32])
        );
    }
    drop(second);
    assert!(!backend.is_payment_event_stream_active());
}

#[tokio::test]
async fn cancellation_interrupts_a_full_event_queue() {
    let (backend, client, _dir) = fixture();
    for n in 1..=150u8 {
        backend.db.insert_mint_quote(&[n; 32], "invoice").unwrap();
        client.payments.lock().unwrap().push(payment(
            u32::from(n),
            PaymentDirection::Inbound,
            [n; 32],
        ));
    }
    let stream = backend.wait_payment_event().await.unwrap();
    // The first page fills the 100-event channel; the second page blocks on send.
    tokio::time::timeout(Duration::from_secs(2), async {
        while client.update_pages.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    backend.cancel_payment_event_stream();
    tokio::time::timeout(Duration::from_secs(2), async {
        while backend.is_payment_event_stream_active() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop(stream);
}
