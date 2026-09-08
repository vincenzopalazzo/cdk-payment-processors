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
- Payment event streaming using Lexe long-poll payment updates with
  reference-counted cleanup, mirroring the spark processor's event cleanup.
- Unit tests for settings validation, quote database persistence, fee
  estimation, payment status mapping, payment index parsing, client
  credential decoding, and gRPC server startup behavior.
- `config.toml.example`, `justfile`, `.gitignore`, license files
  (MIT / Apache-2.0), and a crate-local `Cargo.lock`.
