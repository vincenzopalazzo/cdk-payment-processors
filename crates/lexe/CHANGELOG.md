# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- A new `lexe` CDK payment processor backed by the Lexe Rust SDK
  (`lexe = "=0.1.22"`), supporting BOLT11 mint and melt via the CDK
  payment processor gRPC protocol.
- Configuration through `config.toml` and `LEXE_*` / shared `SERVER_*` /
  `TLS_*` environment variables.
- Lexe SDK client credentials support (`LEXE_CLIENT_CREDENTIALS` /
  `lexe.client_credentials`), matching the lexe-mcp credential model.
- Optional BIP39 seed phrase fallback (`LEXE_SEED_PHRASE` /
  `lexe.seed_phrase`) for node signup and provisioning.
- A local redb quote/payment database keyed by BOLT11 payment hash for
  idempotent outgoing payments and status lookup.
- Payment event streaming using paginated Lexe payment updates with
  reference-counted cleanup and replay on reconnect.
- Unit tests for settings validation, quote database persistence, fee
  estimation, payment status mapping, payment index parsing, client
  credential decoding, and gRPC server startup behavior.
- `config.toml.example`, `justfile`, `.gitignore`, license files
  (MIT / Apache-2.0), and a crate-local `Cargo.lock`.

### Changed

- Match the advertised `sat` unit in all new requests and the mint example.
- Align fee reserve defaults with LDK Server: 10,000 ppm (1%) and 2 sats.
- Reject new outgoing requests with `max_fee_amount` because Lexe SDK 0.1.22
  cannot enforce the cap. Ordinary capped CDK melts remain unsupported until
  the SDK exposes this capability; incoming payments are unaffected.

### Fixed

- Sum outgoing principal and fees in millisatoshis before rounding the total
  up for sat quotes; preserve exact msat totals for legacy attempts.
- Atomically persist submission intent and the original quote's event owner;
  unused quotes stay unpaid and concurrent/restarted retries never resubmit
  an ambiguous attempt. Migrate legacy quote records conservatively.
- Recover missing payment indexes across all history pages and match only
  outbound payments; reconcile SDK errors as well as timeouts.
- Replay cached payment updates on reconnect/restart, retry failed event
  mappings, and allow cancellation while the event queue is full.
- Include Lexe in CI checks when the shared workflow changes.
- Correct configuration examples and document TLS self-check behavior.
- Add mocked payment lifecycle tests and database migration/concurrency tests.
