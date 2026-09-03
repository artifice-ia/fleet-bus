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

**Normative, and read off the running broker on 2026-09-02 rather than off a
plan.** Every subject below exists in `nats.conf`'s per-user permissions today.

Per-bot subjects — each bot owns three:

- `fleet.<bot>.request` — direct requests targeting `<bot>`
- `fleet.<bot>.result` — replies addressed to `<bot>`, carrying `in_reply_to`
- `fleet.<bot>.status` — heartbeats and status pings originating from `<bot>`

Broadcast subjects:

- `fleet.broadcast.<kind>` — fleet-wide broadcasts (subscribers optional per bot)

Client inboxes:

- `_INBOX_<bot>.>` — the NATS request/reply inbox, per the `inboxPrefix` rule
  below. **Not an application subject**; nothing addresses a bot there.

### 6.1 — The reply lane TODAY is `.result`. Removal happens in FB-3.

**A publisher selects the subject from `in_reply_to`, not from the kind:**

```
to == null                 → fleet.broadcast.<kind>
to != null, no in_reply_to → fleet.<to>.request
to != null, in_reply_to    → fleet.<to>.result
```

That rule is implemented identically in `pick_publish_subject` (`bus.py`) and in
the reply path of `fleet-bus.ts`, and the TypeScript client's correlated
`request(wait: true)` **resolves only on a matching `.result`** — a reply
delivered anywhere else leaves the caller waiting until timeout.

**This clause describes the live wire, not the end state.** `.result` is used by
both deployed implementations, is granted in every bot's subscribe and publish
permissions, and as of FB-1 has a JetStream stream (`FLEET_RESULT`) capturing
it. **The subject class stays until FB-3 removes it** — §6.2 has the target and
the migration. Everything in §6 is live-state documentation: it says what a
publisher must do to be understood today, and it stops being true at the
cutover, not before.

### 6.2 — The reply lane is mid-migration, and yugo is ahead of it

`yugo/fleet_bus.py` publishes replies as ordinary `.request` envelopes carrying
`in_reply_to`, and does not use `.result`.

**That is the ratified target, not a defect.** Yugo's SPEC §4.7 — the v0.7 authz
map — states plainly: *"`.result` subject class is REMOVED (was in v4 spec).
Replies travel as ordinary `.request` envelopes with an `in_reply_to` field;
coordinator gates them by matching `root_id`."* Fernando confirmed the removal
on 2026-08-29. Yugo implements the end state early; this document describes the
wire the fleet runs today. **Both are correct about different points in time.**

An earlier revision of this section had it backwards — it called yugo the
divergence and said the fix belonged there. It is recorded rather than quietly
replaced, because the reasoning that produced it will recur: two implementations
agreeing is not evidence when the third is the one implementing the approved
change, and a majority is not an argument about direction.

**What does hold is the interop consequence.** Until the migration completes,
**a correlated `request(wait: true)` from a `fleet-bus.ts` peer to a yugo bot
cannot resolve.** Yugo answers on `.request`, the caller listens on `.result`,
and the exchange ends in a timeout indistinguishable from an unresponsive bot.
Replies sent *to* yugo on `.result` are subscribed but deliberately not injected.

Nothing is broken in production only because yugo is not deployed to a fleet bot.

### 6.2.1 — The migration cannot be "one adapter per PR, in any order"

Yugo §15 defines FB-3 as separate per-adapter PRs that land in any order. **For
the reply lane specifically, that ordering strands peers**, and the section
above would otherwise warn about a hazard its own plan creates:

- a migrated adapter replies on `.request`, where an unmigrated receiver injects
  it as a new request instead of resolving the waiter that is still listening
  on `.result`;
- an unmigrated adapter replies on `.result`, which a migrated receiver has
  deleted and had revoked.

Either direction breaks a correlated call, and both are live the moment the
first adapter flips.

**The resolution is a compatibility phase — expand, migrate, contract.** Within
each phase adapters proceed in any order; **between phases there is a global
completion gate.** Two of them, not one:

1. **Expand (per adapter, any order).** Every adapter *receives and correlates*
   on **both** lanes. Nothing changes about what it publishes, so this step is
   invisible on the wire and cannot strand anyone.
   **Gate 1: every adapter completes Expand before any adapter begins
   Migrate.**

2. **Migrate (per adapter, any order, after Gate 1).** The adapter switches
   reply *publication* to `.request` + `in_reply_to`. Order within the phase is
   free because Gate 1 left **every** peer able to receive either lane —
   "every" is the load-bearing word, and it is why the gate is not optional.
   Without it: A expands, then migrates before B has expanded, A replies to B
   on `.request`, B's old receive path injects it as a fresh request, and B's
   waiter on `.result` times out. That is the same stranding this section
   exists to prevent, reintroduced by the ordering freedom rather than the
   change.

   **Gate 2: every adapter completes Migrate before any adapter begins
   Contract.**
3. **Contract (per adapter, any order, after Gate 2).** Delete the
   `.result` **subscription and handler** and any result-specific state, and
   revoke `.result` authz in **both** directions — publish as well as
   subscribe. Revoking subscribe alone would leave every bot able to publish to
   a channel nothing gates. Once no adapter subscribes it, remove the
   `FLEET_RESULT` stream.

**Keep the outbound waiter ledger.** An earlier revision of this section said to
delete it along with the `.result` receive path, copied from yugo's §15 erratum
without checking what it holds. That ledger is **subject-independent
correlation state**: expanded adapters need it to resolve `in_reply_to` on both
lanes during step 1, and after step 2 it is still the mechanism that resolves a
reply arriving on `.request`. Deleting it does not remove a `.result`
dependency — it removes `request(wait: true)` from the target topology
altogether, which is a separate breaking change and would have to be decided as
one.

**What is global is the GATES, not any operation.** No step is an atomic
fleet-wide cutover — cross-repo code deletion could not be one in any case.
What must hold globally is a precondition before each phase begins, and each
gate exists because the phase after it is only safe once the phase before it is
universally true. **"Any order" survives inside all three phases; what does not
survive is starting a phase early.**

An earlier revision named only Gate 2, and claimed Migrate was safe in any
order "because step 1 left every peer able to receive either lane" — a property
of *all* adapters having expanded, asserted as though one adapter expanding
established it. Both gates are the same error caught twice: a phase's safety is
a fleet-wide fact, and a per-adapter step does not create one.

Until step 3 lands, `.result` stays in this document, because it is what the
wire does. Tracked as **INF-036**.

### 6.3 — JetStream capture (FB-1, applied 2026-09-02)

Two streams persist the request and reply lanes. Applied to the live broker;
these are the running values, not a proposal.

| Stream | Subjects | Retention | Per subject | Discard | Storage |
|---|---|---|---|---|---|
| `FLEET_REQUEST` | `fleet.*.request` | 7 days | 100,000 | old | file |
| `FLEET_RESULT` | `fleet.*.result` | 7 days | 100,000 | old | file |

`fleet.*.status` and `fleet.broadcast.>` are deliberately **not** captured:
heartbeats are presence, and replaying a broadcast to a bot that was offline
re-delivers an announcement whose moment has passed.

Three consequences worth stating, because each one was verified rather than
assumed:

- **A core publish still lands in the stream.** A client with no JetStream
  permission at all publishes normally and its messages are captured, so bots
  migrate to durable consumers one at a time. There is no flag day.
- **Every user needs `$JS.API.>` on both verbs** to bind a consumer. Without it
  the client does not fail cleanly — the JetStream request **times out**, which
  reads as a hung bot rather than a misconfigured one.
- **The client's custom `inboxPrefix` applies to JetStream API calls too.** An
  ops script that omits it is refused by the same permission rule.

Per-user NATS permissions:

- Each bot has NATS user `<botname>` with password from `~/.claude/fleet-bus-tokens.conf`
- Subscribe: `fleet.<self>.request`, `fleet.<self>.result`, `fleet.<self>.status`, `fleet.broadcast.>`, `_INBOX_<self>.>`, plus optional read-only observation subjects
- Publish: `fleet.*.request`, `fleet.*.result`, `fleet.<self>.status` (self only), `fleet.broadcast.>`, `_INBOX_<self>.>`
- **Both verbs additionally grant `$JS.API.>` (FB-1).** JetStream's API is a
  request/reply exchange, so a consumer needs it on subscribe *and* publish;
  granting one is the same as granting neither.
- Console (supervision) user: subscribe `fleet.>`; publish `{ deny: [">"] }`.
  **Console therefore cannot use the JetStream API at all** — it is
  publish-denied by design, and the API needs publish. A read-only observer
  cannot inspect streams. Acceptable while nothing depends on it; it needs a
  decision before FB-4's gate suite wants an observer.

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
