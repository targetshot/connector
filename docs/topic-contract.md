# ts-connect Topic Contract

This document separates product topics from MirrorMaker 2 / Kafka Connect internals.

## Product Topics

- `ts.sds-test.*`
  - Current business topics in this environment.
  - These are the routed/normalized topics that should remain stable for downstream consumers.
- `ts.raw.*`
  - Internal raw Debezium topics on the local cluster.
  - Used for local buffering and transformation, not as the long-term cloud-facing contract.

## Required Internal Topics

- `_ts_mm2_v3_configs`
- `_ts_mm2_v3_offsets`
- `_ts_mm2_v3_status`

These are Kafka Connect worker state topics for the MirrorMaker 2 runtime. They are required while MM2 is active and should not be deleted as part of topic cleanup.

If an older environment still uses `ts_mm2_v3_*` without the leading underscore, treat them the same way: they are still internal worker-state topics.

## MM2 Internal / Legacy Topics

- `heartbeats`
- `local.heartbeats`
- `local.checkpoints.internal`

These are MM2 internal topics. They are not product/business topics.

In the current `ts-connect` config:

- heartbeats are disabled
- checkpoints are disabled
- offset-sync emission is disabled
- group offset sync is disabled

That means these topics should not be required for the intended one-way `local -> remote` product replication path.

## Cleanup Guidance

Safe default:

- keep `ts.sds-test.*`
- keep `ts.raw.*`
- keep the MM2 worker state topics (`_ts_mm2_v3_*` or legacy `ts_mm2_v3_*`)
- treat `heartbeats`, `local.heartbeats`, and `local.checkpoints.internal` as internal/legacy noise unless a live MM2 verification shows they are still recreated on startup

Recommended cleanup process for the legacy MM2 topics:

1. Stop `ts-mirror-maker`.
2. Start it again with the current config and verify the replication path still works.
3. If `heartbeats`, `local.heartbeats`, or `local.checkpoints.internal` are not recreated and no consumer depends on them, delete them once from the Kafka cluster.
4. Do not delete the MM2 worker state topics while MM2 is active.

## Why This Split Exists

- product topics should be understandable for operators and consumers
- MM2/Kafka Connect internal topics are implementation details
- reducing topic noise should start by classifying and hiding/cleaning internals, not by renaming the product contract blindly
