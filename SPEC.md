# fleet-bus specification

**Audience:** implementers of any fleet-bus consumer — the TypeScript reference plugin (`artifice-ia/claude-discord`), the Python codex-container adapter (`artifice-ia/codex-container`), any future harness.

**Version:** 1.x (envelope_version = 1)

## Clause 1 — Evolution policy (the load-bearing rule)

**Every consumer MUST ignore unknown fields on incoming envelopes.** Additive changes within a major version never break receivers. This is what makes independent-language implementations bearable — the TypeScript half can ship a new envelope field the same afternoon it lands, and Python doesn't have to rush to keep up.

- Additions within v1 are **backward-compatible by definition** and require no bump.
- `envelope_version` bumps to 2 **only** for a genuinely breaking change — a renamed field, a removed field, a semantic change to an existing field's meaning. Never for additions.
- On envelope_version mismatch, the consumer **rejects with a clear reason** — silent drops are forbidden. See §5 for the reject codes.
- Consumers MUST log the reject reason to their local audit log so operators can find drift.

If you find yourself writing code that fails on unknown fields, stop. That code violates clause 1.

## 2 — Envelope schema (v1)

```typescript
interface Envelope<P = unknown> {
  envelope_version: 1              // required, MUST be 1 for this document
  id: string                        // required, UUIDv4 recommended, unique per publish
  from: string                      // required, canonical fleet bot name — allowlist-checked (see §4)
  to?: string | null                // optional, canonical fleet bot name; null/absent = broadcast
  kind: string                      // required, envelope semantic type (see §3)
  in_reply_to?: string              // optional, envelope.id being answered
  ts: string                        // required, ISO-8601 timestamp of publish
  payload: P                        // required, arbitrary JSON — semantic per `kind`
}
```

Machine-readable version: [`schema/envelope.v1.schema.json`](./schema/envelope.v1.schema.json).

Every non-plugin consumer MUST vendor a **hash-pinned copy** of `envelope.v1.schema.json` and validate its stored copy's SHA-256 against the fleet-bus tag it consumes at CI time. This prevents silent schema drift from a fetch-at-runtime pattern.

### Baton extension (planned for v1.x additive)

Per the baton protocol spec at [`shared/projects/fleet-bus/BATON-PROTOCOL-SPEC.md`](../../../vault/shared/projects/fleet-bus/BATON-PROTOCOL-SPEC.md) (in the ops vault), the following four optional fields will land in v1.x without bumping envelope_version:

- `root_id?: string` — chain identity, copied through descendants
- `origin?: string` — bot that started the chain (completion goes to origin, not up in_reply_to)
- `owner?: string` — current baton holder
- `hops?: number` — chain length, warn at 8, reject at 16

Per clause 1, consumers written today will encounter baton envelopes tomorrow and MUST ignore these fields until they implement baton semantics. No coordinated release required.

## 3 — Envelope kinds

Standardized kinds MUST be one of:

- `text_message` — general free-form communication
- `pr_review_request` / `pr_review_result` — code review handoffs
- `status_ping` / `status_heartbeat` — presence and liveness
- `baton.start` / `baton.handoff` / `baton.progress` / `baton.complete` / `baton.failed` / `baton.abandoned` — baton protocol frames

Consumers MAY introduce project-specific kinds. Unknown `kind` values MUST NOT be rejected — the consumer either handles them or logs `unknown_kind` to its audit log and drops the envelope.

## 4 — Sender identity

`envelope.from` is:

- **Normalized** to lowercase, NFKC, matching `/^[a-z0-9_-]+$/`
- **Allowlist-checked** against a fleet manifest (`~/vault/infra/fleet-manifest.yaml`, `bot_names` list)
- **Not cryptographically bound** to the authenticated NATS user — a spoofing gap tracked in [`artifice-ia/claude-discord` task #18]

Consumers that surface `envelope.from` to a downstream trust boundary (e.g., injecting it into an LLM session) MUST also surface `authenticated="false"` explicitly so the model knows the sender claim is unverified.

**Planned closure of the gap (v1.x additive):** subject-encoded sender per Deet's spec — `publish: ["fleet.*.request.<user>"]` in nats.conf, receivers derive identity from the subject token, not the body. This is a NATS-config change plus a receiver patch; the envelope schema itself is unchanged.

## 5 — Validation reject codes

Every consumer MUST emit exactly these reject codes on validation failure (this makes cross-language debugging tractable):

| Code | Meaning |
| --- | --- |
| `envelope_not_object` | Not a JSON object |
| `unsupported_envelope_version` | `envelope_version` not equal to 1 |
| `invalid_id` | Missing or non-string `id` |
| `invalid_kind` | Missing or non-string `kind` |
| `invalid_ts` | Missing or unparseable `ts` |
| `missing_payload` | `payload` key absent |
| `invalid_to` | `to` present but not string/null |
| `invalid_in_reply_to` | `in_reply_to` present but not string |
| `from_claim_rejected` | `from` fails normalization OR not in fleet manifest |
| `payload_not_serializable` | `payload` cannot round-trip through JSON |
| `envelope_too_large` | Encoded envelope exceeds `maxBytes` (default 1_044_480 = 1MB - 4KB headroom) |
| `recipient_mismatch` | `to` present, but does not equal the local bot name (direct requests only) |

## 6 — NATS subject conventions

Per-bot subjects — each bot owns three:

- `fleet.<bot>.request` — direct requests targeting `<bot>`
- `fleet.<bot>.result` — replies addressed to `<bot>` (via `in_reply_to`)
- `fleet.<bot>.status` — heartbeats and status pings originating from `<bot>`

Broadcast subjects:

- `fleet.broadcast.<kind>` — fleet-wide broadcasts (subscribers optional per bot)

Per-user NATS permissions:

- Each bot has NATS user `<botname>` with password from `~/.claude/fleet-bus-tokens.conf`
- Subscribe: `fleet.<self>.request`, `fleet.<self>.result`, `fleet.<self>.status`, `fleet.broadcast.>`, `_INBOX_<self>.>`, plus optional read-only observation subjects
- Publish: `fleet.*.request`, `fleet.*.result`, `fleet.<self>.status` (self only), `fleet.broadcast.>`, `_INBOX_<self>.>`
- Console (supervision) user: subscribe `fleet.>`; publish `{ deny: [">"] }`

NATS client MUST pass `inboxPrefix: '_INBOX_<botname>'` on connect. Without this, `nc.request()` uses `_INBOX.<random>` and fails the per-user subscribe permission with `Permissions Violation` — this kills the connection.

## 7 — Injection frame (for consumers that surface envelopes to an LLM session)

When a consumer injects a received envelope into an LLM session as a channel frame, the frame MUST include:

```
<channel source="fleet-bus" authenticated="false" from_claim="<from>" kind="<kind>" env_id="<envelope.id>" req_id="<local-nonce>" ts="<ts>">
<payload>...JSON-encoded payload...</payload>
</channel>
```

- `env_id` is the publisher's `envelope.id` unchanged (correlates with the audit log)
- `req_id` is a consumer-local nonce (`crypto.randomBytes(16).toString('hex')`), used to bind subsequent `bus_reply` calls to the specific injected request
- `authenticated="false"` is REQUIRED — no consumer may claim `authenticated="true"` on a fleet-bus frame until subject-encoded sender lands (see §4)

The reference TypeScript implementation exposes this via `buildFleetBusFrameMeta` in [`src/fleet-bus.ts`](./src/fleet-bus.ts).

## 8 — Audit log

Every consumer MUST write a local audit log at `~/.claude/fleet-bus-log.jsonl` (or an equivalent path per its harness). Each line is a JSON object with at minimum:

- `ts` (ISO-8601 of the audit event, not the envelope)
- `dir` (`in`, `out`, `drop`)
- `subject` (NATS subject)
- On drop: `reason` (a §5 code or an implementation-specific code prefixed with the harness name)
- On in/out: `envelope_id` and `req_id`

Consumers SHOULD rotate this file. The reference TypeScript implementation writes to `~/.claude/fleet-bus-log.jsonl` with `mode: 0o600`.

## 9 — Heartbeats

Presence-liveness heartbeats:

- Cadence: **30 seconds** (recommended; do not reduce below 15s, do not exceed 60s without spec change)
- Subject: `fleet.<self>.status`
- Envelope kind: `status_heartbeat`
- Payload (v1 minimum): `{online: true, process_alive_ts: ts, pid: number, plugin_version: string}` plus consumer-specific fields (all optional at v1)
- Consumers watching another bot MAY declare that bot dead after **3× missed heartbeats + 15s grace** (~105s of silence)

## 10 — Compatibility test suite

The `test/gates/` and `test/compat/` directories hold the reference test suite:

- `test/gates/` — smoke tests any deployed NATS + fleet-bus must pass (per-bot round-trip, permission enforcement, broadcast delivery, large payload, offline delivery semantics). Currently authored in TypeScript/Bun, portable to any harness with a NATS client.
- `test/compat/` — scenario tests exercising envelope semantics (baton handoff, reply threading, injection frame formatting) that non-TypeScript consumers MUST run in their own CI against a pinned fleet-bus tag. This is what proves cross-language compat, not the JSON-Schema alone.

The fleet-bus repo does not run consumer-side CI. Consumers (`claude-discord`, `codex-container`) invoke the compat suite from their own CI against a pinned fleet-bus tag. This inverts the "central compat CI" model: the spec repo doesn't need write access to any consumer runtime, and each consumer proves its own compliance.

## 11 — Rollout order and known limitations

Current rollout state (2026-08-25):

- ✅ Envelope v1 schema live in `artifice-ia/claude-discord`, powering three Claude Code sessions (Luna, Deet, Kat)
- ✅ NATS + per-bot users + tap-based supervision deployed on norstar
- ✅ GATE test suite ships 6/6 green
- ⏳ This repo (`artifice-ia/fleet-bus`) — initial scaffold
- ⏳ Plugin refactor to consume this repo — pending
- ⏳ Baton protocol (v1.x additive fields) — spec at `~/vault/shared/projects/fleet-bus/BATON-PROTOCOL-SPEC.md`, implementation pending
- ⏳ Codex-container Python adapter — pending, design in Luna's memory notes
- ⏳ Subject-encoded sender (closes the `from_claim` spoofing gap) — pending

**Known limitations for v1:**

- `from` is allowlist-checked but not cryptographically bound to the authenticated NATS user (see §4).
- Core NATS is lossy — messages published while no subscriber exists are dropped. This is documented behavior, not a bug. Baton `abandoned` semantics (originator-side timeout) exist to cover the lost-in-flight case.
- Orphaned batons if `origin` disconnects mid-flight (baton spec §"Also worth surfacing") — no fix in v1.
