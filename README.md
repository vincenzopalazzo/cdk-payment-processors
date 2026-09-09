# CDK Payment Processors

A collection of self-contained Cargo projects implementing CDK Payment
Processors (`MintPayment` interface over gRPC).

See [CONTRIBUTING.md](CONTRIBUTING.md) for repository-wide development and
contribution guidelines.

| Processor | Unit | BOLT11 | BOLT12 | On-chain | Custom |
| --- | --- | --- | --- | --- | --- |
| [Bark](crates/bark/README.md) | `sat` | ✅ | ❌ | ✅ | `arkoor` |
| [LDK Server](crates/ldk-server/README.md) | `msat` | ✅ | ✅ | ❌ | - |
| [LNbits](crates/lnbits/README.md) | `sat` | ✅ | ❌ | ❌ | - |
| [Lexe](crates/lexe/README.md) | `sat` | ✅ | ❌ | ❌ | - |
| [Spark](crates/spark/README.md) | `sat` | ✅ | ❌ | ❌ | - |
| [Template](crates/template/README.md) | `sat` | ✅ | ❌ | ❌ | - |

BOLT11 features:

| Processor | Amountless Invoices | MPP | Invoice Descriptions |
| --- | --- | --- | --- |
| [Bark](crates/bark/README.md) | ❌ | ❌ | ✅ |
| [LDK Server](crates/ldk-server/README.md) | ✅ | ❌ | ✅ |
| [LNbits](crates/lnbits/README.md) | ❌ | ❌ | ✅ |
| [Lexe](crates/lexe/README.md) | ❌ | ❌ | ✅ |
| [Spark](crates/spark/README.md) | ❌ | ❌ | ❌ |
| [Template](crates/template/README.md) | ❌ | ❌ | ✅ |

Lexe's pinned SDK cannot enforce outgoing fee caps. Incoming BOLT11 payments
are supported, but new capped melts are rejected; see its
[payment limitations](crates/lexe/README.md#behavior-notes).

## Project structure

```text
crates/
├── bark/        # Payment processor backed by a Bark wallet
├── ldk-server/  # Payment processor backed by an LDK Server node
├── lexe/        # Payment processor backed by a Lexe managed node
├── lnbits/      # Payment processor backed by an LNbits wallet
├── spark/       # Payment processor backed by a Spark wallet
└── template/    # Starting point for integrating a new payment backend
```

Each processor has its own `Cargo.toml`, `Cargo.lock`, configuration example,
and documentation. The crates intentionally do not share a Cargo workspace,
so dependency resolution and reproducible builds are independent.

Check or test every crate from the repository root:

```bash
for manifest in crates/*/Cargo.toml; do
  cargo check --locked --manifest-path "$manifest"
  cargo test --locked --manifest-path "$manifest"
done
```

Run an individual processor from its directory:

```bash
cd crates/template
cargo run --release
```
