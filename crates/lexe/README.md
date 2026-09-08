# CDK Payment Processor - Lexe

A CDK payment processor backed by a [Lexe](https://github.com/lexe-app/lexe-public)
managed non-custodial Lightning node (SGX). Exposes the node to `cdk-mintd`
over the CDK payment processor gRPC protocol, with BOLT11 support.

Authentication follows the same model as
[lexe-mcp](https://github.com/vincenzopalazzo/lexe-mcp): a single base64
"SDK client credentials" blob created in the Lexe app (Menu → SDK clients).
An optional BIP39 seed phrase is supported as a fallback credential.

```text
cdk-mintd (payment_backend = "grpcprocessor")
  -> gRPC        cdk-payment-processor-lexe (this crate)
  -> Lexe SDK    Lexe managed node (gateway + node)
```

## Usage

```bash
cp config.toml.example config.toml   # fill in client_credentials (or seed_phrase)
cargo run --release
```

Point `cdk-mintd` at the processor:

```toml
[payment_backend]
backend = "grpcprocessor"
unit = "msat"

[grpc_processor]
supported_units = ["msat"]
address = "127.0.0.1"
port = 50051
allow_insecure = true
```

CDK 0.18 chooses the URI scheme from `tls_dir`, so `address` must not contain
`http://` or `https://`. Existing mint operators must migrate and initialize
their database-backed configuration before starting 0.18; follow the
[CDK v0.18 migration guide](https://github.com/cashubtc/cdk/blob/main/docs/migrations/v0.18.md).

## Configuration

See [config.toml.example](config.toml.example). A `config.toml` in the current
directory is optional, and environment variables take precedence over its
values.

| `config.toml` key | Environment variable | Default |
| --- | --- | --- |
| `address` | `SERVER_ADDRESS` | `127.0.0.1` |
| `port` | `SERVER_PORT` | `50051` |
| `tls_enable` | `TLS_ENABLE` | `false` |
| `allow_insecure` | `ALLOW_INSECURE` | `false` |
| `tls_cert_path` | `TLS_CERT_PATH` | `certs/server.crt` |
| `tls_key_path` | `TLS_KEY_PATH` | `certs/server.key` |
| `tls_client_ca_path` | `TLS_CLIENT_CA_PATH` | `certs/ca.pem` |
| `lexe.client_credentials` | `LEXE_CLIENT_CREDENTIALS` | Required (xor) |
| `lexe.seed_phrase` | `LEXE_SEED_PHRASE` | Required (xor) |
| `lexe.network` | `LEXE_NETWORK` | `mainnet` |
| `lexe.data_dir` | `LEXE_DATA_DIR` | `.data/lexe` |
| `lexe.fee_reserve_ppm` | `LEXE_FEE_RESERVE_PPM` | `100` |
| `lexe.fee_reserve_min_sat` | `LEXE_FEE_RESERVE_MIN_SAT` | `1` |
| `lexe.payment_timeout_secs` | `LEXE_PAYMENT_TIMEOUT_SECS` | `300` |

Exactly one of `client_credentials` / `seed_phrase` must be configured. The
client credentials are the same base64 blob lexe-mcp uses as
`Authorization: Bearer <credentials>`; the processor decodes it and uses it
to talk to the Lexe node with mTLS-attested, token-refreshed connections.

With TLS enabled, `tls_client_ca_path` must contain the CA certificate that
signed the mint's `client.pem`; clients without a trusted certificate are
rejected. Configure the mint's `[grpc_processor].tls_dir` with `ca.pem`,
`client.pem`, and `client.key`.

Without TLS, startup fails unless `allow_insecure = true` (or
`ALLOW_INSECURE=true`) is explicitly configured. The opt-in permits
cleartext on any bind address so it can be used in containers; startup logs a
warning with the effective address and a stronger exposure warning for
non-loopback binds. Configure mutual TLS whenever the network is not fully
trusted.

## Behavior notes

- **Incoming (mint):** the processor creates BOLT11 invoices on the Lexe
  node, keyed by payment hash in a local redb database, and reports
  `PaymentReceived` events via an index-cursor polling stream
  (`wait_for_next_payment` with backoff).
- **Outgoing (melt):** Lexe has no fee-estimate API and no client idempotency
  token, so quotes use a configurable fee estimate
  (`max(ppm * amount, min_sat)`, real fee is reported once the payment
  settles), and retry safety comes from the local database: a stored Lexe
  payment index is re-queried instead of paying again. `pay_invoice` blocks
  to a terminal state; on timeout the payment is reported `Pending` and
  recovered by scanning recent payments by payment hash.
- **Status checks:** `check_incoming_payment_status` / `check_outgoing_payment`
  resolve stored Lexe payment indexes and map `completed`/`failed`/`pending`
  to the CDK quote states.

## Startup self-check

After binding, the processor calls its own `GetSettings` from the local host
(using loopback for unspecified addresses such as `0.0.0.0`) and **exits
non-zero** if it does not answer. This fails fast on port conflicts instead of
looking healthy while another service owns the port.

## Notes

- Depends on the `lexe` Rust SDK (crates.io, pinned to `=0.1.22`). The crate
  ships a library (shared backend, used by its unit tests) plus the gRPC
  server binary; it is not published to crates.io.
- BOLT11 only; BOLT12 and on-chain payment options are not supported yet.
- A `Cargo.lock` is committed for reproducible standalone builds.
