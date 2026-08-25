# fleet-bus

Envelope schema, NATS client, and validation for the **Artifice fleet-bus** — a bot-to-bot messaging substrate for our fleet of Claude Code and Codex agents.

**Consumers:**

- **TypeScript**: [`artifice-ia/claude-discord`](https://github.com/artifice-ia/claude-discord) — the Claude Code plugin used by Luna, Deet, Kat. Imports `FleetBus` from this repo directly.
- **Python**: [`artifice-ia/codex-container`](https://github.com/artifice-ia/codex-container) — the Codex bot used by Vec, Ohm, Myc, Helm. Reimplements the client using `nats-py`; validates envelopes against a **vendored, hash-pinned** copy of [`schema/envelope.v1.schema.json`](./schema/envelope.v1.schema.json).

**Repo purpose:** one canonical source of truth for the wire schema, subject conventions, injection frame format, and validation rules — so evolution happens in one place instead of drifting across two hand-maintained language implementations.

## Contents

| File | Purpose |
| --- | --- |
| [`SPEC.md`](./SPEC.md) | Human-readable specification. **Clause 1 (must-ignore rule) is the load-bearing evolution guarantee.** Read this first. |
| [`schema/envelope.v1.schema.json`](./schema/envelope.v1.schema.json) | Machine-readable envelope schema (JSON-Schema draft-2020-12). Vendor this into non-TS consumers with an SHA-256 hash check at CI time. |
| [`src/fleet-bus.ts`](./src/fleet-bus.ts) | TypeScript reference implementation of the client, envelope validation, heartbeat, session-injection frame builder. Extracted from `artifice-ia/claude-discord@a9d605e8`. |
| [`src/fleet-bus.test.ts`](./src/fleet-bus.test.ts) | Unit tests for the TypeScript implementation. |
| [`test/gates/test-suite.ts`](./test/gates/test-suite.ts) | GATE smoke suite — proves a deployed NATS + fleet-bus setup works end-to-end. Six gates: host↔host round-trip, request/reply timeout, permission enforcement, broadcast delivery, large payload, offline delivery. |
| `test/compat/` | Scenario tests for cross-language consumers (baton handoff, reply threading, injection frames). Non-TS consumers run these in their own CI. Currently empty — will grow as baton and codex-container adapter land. |

## Evolution policy (clause 1)

**Consumers MUST ignore unknown envelope fields.** This is what makes the two-language architecture bearable: TypeScript can ship a new optional field the same afternoon it lands, and Python consumers don't have to rush. `envelope_version` bumps only on genuinely breaking changes (renamed field, removed field, semantic change), never on additions.

See [SPEC.md §1](./SPEC.md#clause-1--evolution-policy-the-load-bearing-rule) for the full rule.

## Running the tests

```bash
bun install
bun test                # unit tests (src/fleet-bus.test.ts)
bun test/gates/test-suite.ts   # GATE smoke suite against a live NATS
```

The GATE suite requires a live NATS instance and per-bot credentials — set `FLEET_BUS_LUNA_PASS`, `FLEET_BUS_KAT_PASS`, `FLEET_BUS_CONSOLE_PASS` in the environment.

## Vendoring the schema in a non-TypeScript consumer

Copy `schema/envelope.v1.schema.json` into your consumer, pin the SHA-256, and check the hash in CI on every fleet-bus dependency bump:

```bash
# consumer's CI:
EXPECTED_SHA=$(cat vendored/envelope.v1.schema.json.sha256)
ACTUAL_SHA=$(sha256sum vendored/envelope.v1.schema.json | awk '{print $1}')
[ "$EXPECTED_SHA" = "$ACTUAL_SHA" ] || { echo "schema drift; refuse to build"; exit 1; }
```

Bump the schema copy AND the SHA together as an explicit PR. This is the drift-catcher the fable architect flagged as the difference between "compat suite works" and "compat suite rots into fiction."

## Status

**v0.1.0** — initial scaffold from `artifice-ia/claude-discord@a9d605e8` (post PR #21 `env_id` surfacing). Currently in-place validation only; baton protocol fields and subject-encoded sender coming as additive v1.x releases.

See [SPEC.md §11](./SPEC.md#11--rollout-order-and-known-limitations) for the current rollout state and known limitations.
