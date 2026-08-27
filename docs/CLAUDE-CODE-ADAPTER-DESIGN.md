# Claude-container fleet-bus adapter — design (v1)

**Target repo:** [`artifice-ia/claude-container`](https://github.com/artifice-ia/claude-container)
**Implements:** [SPEC.md](../SPEC.md) for the Claude Code CLI runtime side.
**Consumers who will run this:** **Optimus** (JL DaaS — Twilio WhatsApp webhook, Python/FastAPI ingress) and **Koi** (Erika familiar — Discord + Telegram, two separate Bun processes). Future bot-process claude-container deploys inherit this shape by default.
**Sibling:** [CODEX-ADAPTER-DESIGN.md](./CODEX-ADAPTER-DESIGN.md) at v7 — same wire, same trust boundary, different runtime.
**Status:** design draft v1, not yet implemented. Explicit REQUEST for another adversarial review pass from Ohm + Codex + Fable now that the v0 architect pass restructured the design.

> **Scope note.** This document covers ONLY the **bot-process** flavor — claude-container bots that spawn `claude --print` per user message (Optimus, Koi, the shape codified in `artifice-ia/claude-container/discord-bot.ts` + `telegram-bot.ts` + `webhook/server.py`). The **persistent Claude Code session** flavor (Luna, Deet, Kat) is a distinct runtime with a different lifecycle and gets its own design document.

---

## Changes since v0 (in response to architect review round 1)

- **§§1–3 restructured around a dedicated bus-adapter process per bot identity** (architect P1-1 + P1-2). v0's "TS ingress owns the bus" model was factually wrong for both flagship consumers: Koi is TWO separate processes (`bun /app/discord-bot.ts &` + `exec bun /app/telegram-bot.ts` per `entrypoint-koi.sh`) — both would connect as the same NATS user, both subscribe `fleet.koi.request`, core NATS delivers to both → two model turns per envelope + shared-dedup race. Optimus's Twilio ingress is Python/FastAPI (`webhook/server.py` under uvicorn), not the imagined `webhook.ts`. New model: **one bus-adapter process per bot identity**, sidecar to the ingress process(es) in the same container. Owns the single NATS connection, the heartbeat, the dedup store, and spawns `claude --print` for bus turns itself. Ingress processes stay bus-free.

- **§4.1 adds own-status filter as pipeline step 1** (P1-3). v0 subscribed `.status` but had no filter → adapter would have spawned a claude subprocess every 30 s off its own heartbeat, forever, on JL's paid tokens. Explicit `envelope.from !== identityName` guard mirrors `codex-container/bus.py:315-317`.

- **§4.1 adds inbound `hops >= 16` pre-model check** (P2-9d). v0's §7 asserted the ceiling but the §4 pipeline never enforced it. Now step 2 of the pipeline, before dedup, before spawn.

- **§4.4 adds broadcast policy** (P1-3 side): follow yugo rule (audit-not-inject on `fleet.broadcast.>` per `fleet_bus.py:1539-1543` and yugo §7.1). On consumer-facing paid bots, one broadcast = one paid claude spawn per subscriber; audit-only is the correct default. Codex-adapter injects; yugo does not; this design follows yugo for cost reasons.

- **New §5 for heartbeat publisher** (P1-4). SPEC §9 MUST — 30 s `status_heartbeat` on `fleet.<self>.status`, minimum payload keys. Adapter-process-owned. Missing entirely from v0.

- **§6 injection frame corrected to SPEC §7 exactly** (P1-5): `authenticated="false"` REQUIRED, attribute name is `from_claim` (not `from`), identifier fields (`env_id`, `req_id`, `root_id`) rendered byte-identical with NO zero-width stripping (audit-log correlation depends on it). `req_id` mint is consumer-local, in the adapter, at receive time.

- **New §3.3 connection supervisor** (P1-6). v0's single `await FleetBus.connect(...)` at boot covered post-connect reconnect (via nats.js `maxReconnectAttempts: -1`) but NOT (a) NATS unreachable at ingress boot — connect rejects, either crashes ingress or leaves bot permanently bus-less; (b) client ever reaching CLOSED state. Explicit outer supervisor loop per `bus.py::_bus_lifecycle` / yugo `run()` at `fleet_bus.py:1218-1240`.

- **§7 rewritten to specify bus-turn model prompt + prompt-engineering rollout step** (P1-7). v0's "stdin = injection frame ONLY" ignored that a `claude --print` fed only an XML frame has never heard of `<BUS>` grammar or the prose-discard rule → adapter would receive envelopes and answer NOTHING, by construction. New §7 specifies a bus-turn system prompt (`--append-system-prompt`) that teaches the tag grammar, auto-`in_reply_to`, tags-not-prose, and no self-address. Rollout step 5 is prompt iteration on the scratch bot before Koi/Optimus.

- **§10 tools-off for v1 bus turns** (P1-8). v0's "tighter `--allowed-tools`" was insufficient: `from` is spoofable fleet-wide until subject-encoded sender lands (Deet's INF-026 open item); these containers hold Erika's memory + JL's tenant data + Twilio-credentialed environment; `Read` alone is exfil via `<BUS>` payload; `WebFetch` is direct exfil to arbitrary URLs. **v1 shape: `--allowed-tools=""` for bus turns**. Tool-bearing bus turns are a v2 item gated on sender authentication.

- **§11 session namespace per Fernando's prior yugo ruling** (P2-12). v0 proposed `bus-<from>-<root_id>` which degenerates to `bus-<from>-undefined` on plain `text_message` envelopes (no baton) and grows unbounded on real baton chains. Fernando's ruling for yugo v0.3b after a Fable review was **per-peer collapsed with a turn cap**: `bus-<from>`, `BUS_HISTORY_MAX_TURNS=10` default. Mirror that. Known limit named explicitly: per-peer isolation is hygiene not a security boundary until subject-encoded sender.

- **§8 declares dedup as forward-compat + fixed SPEC §14 ghost citations** (P2-11). Pre-FB-3 core NATS never redelivers; the SQLite dedup store defends against nothing today. yugo defers dedup to FB-3 (`fleet_bus.py:16-17`). This design ships the store as forward-compat (harmless, ready when FB-3 lands) but says so explicitly. Removed two nonexistent-SPEC-§14 citations; renamed the env var from copy-paste `YUGO_DEDUP_STORE_PATH` to `FLEET_BUS_DEDUP_STORE_PATH`.

- **§11 lock-scope clarified** (P2-13). v0's "shared lock with user-message ingress path" didn't exist in `discord-bot.ts` today AND would have introduced head-of-line blocking (bus flood starves paying humans). New shape: bus turns serialized among themselves via bounded queue + drop policy (audited); user turns untouched.

- **§8 adds SPEC §10 compat suite** (P2-14). Consumer CI MUST run `test/compat/` against the pinned fleet-bus tag. Codex-adapter has it as rollout step 7; v0 missed it.

- **New §12 SPEC compliance matrix** filled from architect's seed (P2-14 + P3-15). 13 rows, one per SPEC clause, each mapping to a design section and a test. Also fixes the citation drift the architect flagged (P3-15) — codex-adapter §11 does NOT cover prose-discard (that's §4/§13) and does NOT cover no-tools (nowhere in codex design); v0 cited both incorrectly.

- **§6 explicit self-address rejection added** (P2-9b). v0's §7 asserted `<BUS>` tag may not address self but §6's validation steps 1–6 never enforced it. `bus.py` HEAD also lacks this; yugo enforces it at `fleet_bus.py:1889`. Explicit in this design.

- **§6.7 loop backstop applications** (P2-9a): reword vacuous "no auto-reply to in_reply_to" backstop — this adapter has no auto-reply at all (codex shape: replies only via explicit tags), so state what actually holds.

- **P3 hygiene sweep**: §5's zero-width regex now written in `\uXXXX` escapes (not literal invisible characters); §5 audit-log mode 0600 at create; §13 nats.js-on-Bun as rollout exit criterion; SIGTERM handler awaits drain.

## Open items requiring Fernando's ruling before implementation

- **Reply routing** (P2-10, Deet INF-026): `bus.py::pick_publish_subject` routes replies to `.result` (per fleet-bus SPEC §6); yugo publishes replies to `.request` with `in_reply_to` (per yugo §7) and subscribes `.result` but never injects it. A claude→yugo reply on `.result` is a delivered, audited, silently-ignored dead letter. Implementing SPEC §6 as written = claude↔codex works, claude↔yugo silent. Ruling needed before rollout step 3 (outbound implementation).

---

## 1 — The problem

Claude-container bots (Optimus, Koi) have no bus presence. Their actual ingress paths and processes today:

- **Koi** container: `bun /app/discord-bot.ts &` + `exec bun /app/telegram-bot.ts` per `entrypoint-koi.sh`. **Two separate Bun processes.** Both receive user messages, both spawn `claude --print` subprocesses.
- **Optimus** container: `exec uvicorn server:app` per `entrypoint-webhook.sh`. **One Python/FastAPI process** at `webhook/server.py`, spawning `["claude", "-p", prompt]` via `subprocess.run` (`server.py:365`).

None of these processes has a NATS client. The claude subprocess has no bus tools. No envelope another bot in the fleet could send would reach the LLM.

We want the same shape codex-container's adapter (`bus.py` at HEAD, post-baton-fix) delivers: **received bus envelopes reach the model as a distinct injection frame; the model can publish outbound envelopes via a first-class tag pattern.** Applied to Claude Code CLI's actual runtime, not the wrong runtime v0 imagined.

## 2 — Design summary

**One dedicated bus-adapter process per bot identity**, running as a sidecar in the same container as the ingress process(es). It owns the single NATS connection, the SPEC §9 heartbeat, the dedup store, the outer supervisor loop, and spawns `claude --print` for bus turns itself. Ingress processes stay bus-free — user-facing traffic and bus traffic have zero shared state at the process level.

Rationale for the per-identity process rather than the per-ingress-process model v0 proposed:

1. **Exactly one subscriber per identity, by construction.** Koi has two ingress processes today; both connecting as user `koi` would receive every envelope twice under core NATS. The sidecar model makes this impossible to configure wrong.
2. **Language-agnostic.** Optimus's ingress is Python (uvicorn/FastAPI); Koi's is Bun/TS; a future ingress could be Rust or Go. The bus adapter doesn't have to be embedded in whatever language chose the ingress framework. Recommendation: **Bun for consistency with `bus.ts` in `bazfer/fleet-bus`**, using `nats.js`. Python containers (Optimus) get a Bun bus-adapter sidecar alongside the FastAPI process — one extra process is cheap.
3. **Trust boundary alignment.** Baton discipline (§8, inherited from yugo `fleet_bus.py` HEAD + `bus.py::derive_baton_fields` PR #3 R3 class-fix) requires the adapter to be the sole writer of baton fields. Model output only ever contains `to`/`kind`/`payload`. The adapter-process boundary is the sole-writer boundary.
4. **Concurrency simplification.** No shared lock between bus turns and ingress turns; bus turns bounded queue serialized among themselves; ingress user-facing latency untouched by bus load.

Two subsystems, each independently owned by the adapter process:

- **Inbound**: `nats.js` client (long-lived, one subscription set per bot identity) subscribes `fleet.<bot>.request/result/status` + `fleet.broadcast.>` on adapter-process boot. On envelope: filter own status → hop-ceiling check → schema validate → dedup → render as safely-escaped `<channel source="fleet-bus" ...>` injection frame per SPEC §7 → hand to `spawnClaudeForBusTurn(frame)` with **no tools** (v1 constraint per §10).

- **Outbound**: bus-triggered subprocess stdout is scanned for `<BUS to="..." kind="..." payload="{...}" />` tags via the quote-aware attribute parser from codex-adapter §6 (regex is insufficient — payload JSON contains `>`). Validated envelope goes to NATS via the adapter's owned connection. Per-tag failure isolation.

**No MCP sidecar for v1.** The `<BUS>` tag pattern proves out with the settled codex-adapter shape; a proper MCP-tool interface is a v2 ergonomics upgrade. Deferring keeps this design shippable.

**Feature-flag off by default:** `FLEET_BUS_ENABLED=0` at `.env` shape. If disabled: the adapter process either doesn't start (via compose profile) or starts + logs "disabled" + exits 0. No NATS code paths execute.

## 3 — Bus lifecycle (adapter process)

### 3.1 Boot

The adapter process (`bus-adapter.ts` under `nats.js`, run via `bun`) at start:

```typescript
if (env.FLEET_BUS_ENABLED !== '1') { console.log('fleet-bus disabled'); process.exit(0); }

await runBusSupervisor({
  url:            env.FLEET_BUS_URL,          // nats://nats:4222
  user:           env.FLEET_BUS_USER,         // 'koi' | 'optimus'
  tokenFile:      env.FLEET_BUS_TOKEN_FILE,   // path — read then discard
  manifestPath:   env.FLEET_MANIFEST_PATH,    // /vault/infra/fleet-manifest.yaml
  onEnvelope:     handleInboundEnvelope,
  auditLogPath:   env.FLEET_BUS_AUDIT_LOG,
  inboxPrefix:    `_INBOX_${env.FLEET_BUS_USER}`,   // per reference_nats_inbox_prefix
  heartbeatMs:    30_000,                     // SPEC §9
  dedupStorePath: env.FLEET_BUS_DEDUP_STORE_PATH,
})
```

### 3.2 Erratum inheritance from yugo v0.3a E-1

- Subscribe subjects are `.request` / `.result` / `.status` / `broadcast.>`, NOT `.inbox`. `.inbox` becomes a subject at FB-3 (JetStream migration); pre-FB-3 the per-adapter subscribe is direct.
- `nats.js` (like `nats-py`) invokes the error callback on first failed permissions error against a subject and returns without closing the connection. A bot that mis-subscribes is connected, heartbeating, deaf. **v1 conformance test MUST publish a probe to `fleet.<self>.request` from a second process and assert `onEnvelope` fires**, not just assert connection state. Boot-time single-shot. Alongside, adopt yugo's own-heartbeat-loopback (`no_echo=False`, subscribe own `.status`, assert periodic receipt) for runtime deafness detection.

### 3.3 Connection supervisor loop

Outer while-true loop around the FleetBus lifecycle, per `bus.py::_bus_lifecycle` and yugo `run()` (`fleet_bus.py:1218-1240`):

```typescript
async function runBusSupervisor(config) {
  while (true) {
    try {
      const bus = await FleetBus.connect(config, { maxReconnectAttempts: -1, waitMs: 2000 })
      bus.on('closed', () => { audit('bus_closed_tripwire'); throw new Error('bus closed') })
      await bus.done()   // resolves when internal reconnect exhausts (-1 = never) or 'closed' fires
      audit('bus_supervisor_iteration_ended')
    } catch (err) {
      audit('bus_supervisor_reconnect', { err: String(err) })
      await sleep(2000)
    }
  }
}
```

Handles two paths nats.js internal reconnect doesn't cover on its own: (a) NATS unreachable at adapter boot → connect rejects → supervisor waits 2 s, retries indefinitely (adapter never crashes, ingress unaffected); (b) client reaches CLOSED → supervisor treats as a full reset, audits, reconnects. `SIGTERM`/`SIGINT` handlers `await bus.drain()` cleanly before exit.

## 4 — Inbound envelope → claude subprocess

`handleInboundEnvelope(env)` — called by the FleetBus with a schema-validated envelope. Pipeline steps, in order:

### 4.1 Filter own status (P1-3 guard)

```typescript
if (env.from === identityName && env.kind === 'status_heartbeat') {
  return  // never inject own heartbeats. silent no-op, no audit.
}
```

Same discipline `bus.py:315-317` codified after codex-adapter's own iteration.

### 4.2 Pre-model hop ceiling

```typescript
if (typeof env.hops === 'number' && env.hops >= 16) {
  audit('claude_adapter_hops_exhausted_inbound', { env_id, from: env.from, hops: env.hops })
  return
}
```

Before dedup, before spawn. Peer-published envelopes at `hops >= 16` never reach the model.

### 4.3 Broadcast policy

```typescript
if (env.subject?.startsWith('fleet.broadcast.')) {
  audit('claude_adapter_broadcast_received', { env_id, kind: env.kind, topic: env.subject })
  return  // audit-only, no inject. yugo rule (fleet_bus.py:1539-1543).
}
```

Direct envelopes on `fleet.<self>.request/result` reach the model; broadcasts are audit-visible via the tap but do not spawn paid claude subprocesses on consumer-facing bots.

### 4.4 Envelope-id dedup

SQLite at `env.FLEET_BUS_DEDUP_STORE_PATH` (default `/var/lib/claude-container/<bot>-dedup.sqlite`, retention ≥ 8d — 1d slack over the JetStream `max_age: 7d` that will be set at FB-1). Second delivery of the same `env_id` is a silent no-op with `claude_adapter_duplicate_delivery` audit.

**Forward-compat declaration**: pre-FB-3 core NATS never redelivers, so this store defends against nothing today. It ships now as ready-when-FB-3-lands, matching the vendored/adapter shape the SPEC will require. yugo defers dedup entirely to FB-3 (`fleet_bus.py:16-17`); this design ships it early because retrofitting later is a bigger diff.

### 4.5 Schema validation + render injection frame

Runtime `ajv` validation using the vendored `envelope.v1.schema.json` from §9. Failed validation → `claude_adapter_schema_invalid` audit, envelope discarded.

Render the SPEC §7-compliant frame (see §6). Frame construction wrapped in try/catch; render failure → `claude_adapter_frame_render_failed` audit, envelope discarded.

### 4.6 Spawn bus turn

- **Session id**: `bus-<from>` (per-peer, per Fernando's yugo v0.3b ruling — see §11). Turn cap `BUS_HISTORY_MAX_TURNS=10` default; older turns evicted from the resumable session's history.
- **Stdin**: the injection frame ONLY, prefixed by the bus-turn system prompt (see §7). No user Discord history, no user prompt.
- **`--allowed-tools=""`**: NO tools (v1 constraint, see §10).
- Stdout captured; stderr logged separately for the audit trail.

### 4.7 Bus-only output

The claude subprocess for a bus turn is bus-only in output:

- `<BUS>` tags in stdout are validated + published (see §6).
- Any prose, other tags (`<CRON>`, `<REPLY>`, image attachments), or non-tag content is dropped with `claude_adapter_prose_from_bus_turn_discarded` audit.

Codex-adapter §4 / §13 codifies this; same rule here for the same reason (bus peer must not be able to inject anything into the Discord/Telegram/Twilio surface).

## 5 — Heartbeat publisher (SPEC §9 MUST)

Owned by the adapter process. Every 30 s, publish to `fleet.<self>.status`:

```json
{
  "envelope_version": 1,
  "id": "<uuidv4>",
  "from": "<identityName>",
  "kind": "status_heartbeat",
  "ts": "<iso8601 utc>",
  "payload": {
    "adapter_version": "<pkg version>",
    "adapter_kind": "claude-container-bus-adapter",
    "pid": <process pid>,
    "uptime_s": <int>
  }
}
```

Publishes only, no reply-expect. Audit line per publish, `dir: "out"`, `subject: "fleet.<self>.status"`, `envelope_id`, `req_id` = locally-minted nonce.

The single adapter process per identity guarantees exactly one heartbeat stream per bot (no double-heartbeat as v0 would have produced on Koi).

## 6 — Frame escaping + outbound `<BUS>` parsing

### 6.1 Injection frame (SPEC §7)

```
<channel source="fleet-bus" authenticated="false" from_claim="<from>" env_id="<env_id>" kind="<kind>" req_id="<local-nonce>" ts="<ts>"
         [root_id="<root_id>" origin="<origin>" owner="<owner>" hops="<hops>"]>
<payload rendered as XML-escaped text>
</channel>
```

- `authenticated="false"` REQUIRED per SPEC §7 (no consumer may claim otherwise).
- `from_claim` (not `from`) — SPEC-mandated attribute name.
- `env_id`, `req_id`, `root_id`: identifier fields, rendered byte-identical, NO zero-width stripping (audit correlation depends on byte-identity — same rule codex-adapter §5 codified after a v3 Codex finding).
- Human-visible fields (`from_claim`, `kind`, payload body): zero-width character stripping using explicit `\uXXXX` escape regex (not literal invisible chars in this doc — P3-16). Codex-adapter zero-width regex ported literally: `[​-‏‪-‮⁠-⁯﻿]`.
- XML attribute escaping for the five entities we emit (`& < > " '`), NOT arbitrary numeric entities.
- `req_id` is a consumer-local nonce minted in the adapter at receive time (SPEC §7). Rendered `req_id` in the frame is this locally-minted value, not any inbound envelope field.

Frame construction wrapped in try/catch (see §4.5). Failure emits `claude_adapter_frame_render_failed` and drops the envelope; does NOT publish an unrendered version.

### 6.2 Outbound `<BUS>` parser (port from codex-adapter §6)

Same shape as `bus.py::process_bus_tags` post-PR#3-R3. Explicit constraints:

1. **Quote-aware attribute parser.** Regex on `<BUS[^>]*>` is insufficient — `payload` attribute holds arbitrary JSON, which contains `>`. Port the stateful attribute parser.
2. **`\<c>` escape preservation.** `\"` unescapes only when the outer quote is `"` (and analogously for `'`). Everything else passes through — JSON `\n`, `\t`, `\"` inside `'...'` attributes survive to `JSON.parse`.
3. **`JSON.parse` reviver rejects `NaN`/`Infinity`.** Not JSON-standard; codex-adapter added `parse_constant` guard in Python + `allow_nan=False` on outbound. TypeScript-side: strict JSON reviver via `JSON.parse(text, revive)`.
4. **Broadcast subject regex uses fullmatch shape** with non-empty dot-separated tokens: `[a-z0-9_-]+(\.[a-z0-9_-]+)*`.
5. **Direct `to` field validated** via `normalize_bot_name` (`^[a-z0-9_-]+$`, must be in the fleet manifest's `bot_names`) BEFORE forming `fleet.<to>.request` — prevents `fleet..request` or `fleet.luna extra.request`.
6. **Self-address rejection.** `<BUS to="<self>" ...>` → `claude_adapter_self_addressed_bus_tag`, drop the tag. This backstop is NOT in `bus.py` HEAD (yugo enforces at `fleet_bus.py:1889`); this design ships it explicitly.
7. **Malformed tag ⇒ stripped from output AND audited.** Tag span recorded even when parse fails; adapter's bus-turn output pipeline already discards prose, so surviving malformed tags are inert, but audit trail records the parse failure.
8. **All adapter-invented audit reasons prefixed `claude_adapter_`.** SPEC §5 reject codes (unprefixed) reserved for the standardized taxonomy; adapter-side codes stay in a distinct namespace.

### 6.3 Reply routing — OPEN DECISION

Two shapes in the fleet today:
- `bus.py::pick_publish_subject` routes replies to `fleet.<to>.result` (per fleet-bus SPEC §6).
- yugo publishes replies to `fleet.<to>.request` with `in_reply_to` set (per yugo §7). yugo subscribes `.result` but never injects it (`fleet_bus.py:1549-1551`).

**Consequence unaddressed**: a claude→yugo reply on `.result` is a delivered, audited, silently-ignored dead letter. claude→codex on `.result` works.

**Ruling from Fernando required BEFORE rollout step 3 (outbound implementation)**. The design will implement whichever wire he picks + reconcile the upstream SPEC to match. Deet flagged this as INF-026 open item.

## 7 — Bus-turn model prompt (P1-7 — was completely missing in v0)

The claude subprocess for a bus turn MUST be primed with a system prompt teaching:

1. The `<BUS to="..." kind="..." payload="{...}" />` grammar (attributes, JSON payload, quote rules from §6).
2. **Tags-not-prose** rule: bus turns MUST end with `<BUS>` tag(s) or empty. Any prose in output is discarded, silently costs the sender a turn. Refusing to answer is a `<BUS to="<from>" kind="text_message" payload='{"text":"<refusal-reason>"}' />`, not silence.
3. `in_reply_to` auto-set: adapter auto-populates `in_reply_to` from the inbound envelope's `id` when the model emits a `<BUS>` addressed back to the sender. Model doesn't set it.
4. No self-address: `<BUS to="<self>" ...>` is dropped (§6.6).
5. No handoffs unless explicitly delegating (baton semantics per §8).
6. The bot's own identity + role from the persona (referenced via `--append-system-prompt` layering on top of user-turn persona).

Delivered via `--append-system-prompt` (not `--print --system-prompt` — need to preserve the persona layered underneath). Prompt content lives at `bus-turn-prompt.md` in the adapter package, versioned.

**Rollout step 5 is explicit prompt iteration on the scratch bot before Koi/Optimus deploy.** Getting the model to reliably emit `<BUS>` tags-only, refuse via `<BUS>` not silence, and observe self-address correctly is empirical work — codex-adapter's approach was to iterate `IDENTITY.md` prompt engineering (codex design §9 rollout step 5); same discipline here.

## 8 — Baton discipline (INHERITED, NOT UP FOR REDESIGN)

**This section replicates yugo `fleet_bus.py::_derive_baton_fields` and codex-container `bus.py::derive_baton_fields` (PR #3 R3 class-fix). It's the settled shape from a 3-round vulnerability iteration. Application-in-adapter is what's under design here; the rules themselves are settled.**

- **Baton fields have exactly one source: the validated inbound envelope.** Model output can carry `to`, `kind`, `payload` on `<BUS>` tags. Any baton key (`root_id`, `origin`, `owner`, `hops`) appearing in a tag → REJECT the whole tag with `claude_adapter_baton_field_in_tag`.
- **Baton fields have exactly one writer: `deriveBatonFields()`.** Single function, one code path, used by every outbound-envelope construction site. TS signature: `deriveBatonFields({envelopeId, identityName, recipient, kind, inbound})` where `inbound` is either the wire envelope this turn was triggered by OR `null` for originating turns (Discord/Telegram/Twilio-triggered where the bot chose to `<BUS>`-out unprompted).
- **On originating turns**: `root_id = envelopeId`, `origin = identityName`, `owner = identityName`, `hops = 0`.
- **On response turns**: `root_id = inbound.root_id ?? inbound.id`, `origin = inbound.origin ?? inbound.from`, `owner = inbound.owner ?? inbound.from`, `hops = (inbound.hops ?? 0) + 1`.
- **On `kind === 'baton.handoff'`**: derive `owner = normalize_bot_name(recipient)`. Refuse handoffs whose recipient is invalid, missing, or broadcast.
- **Post-derivation hop bound check**: if `derivedHops >= 16`, throw `BatonHopsExhausted` before publish.
- **`validateInboundEnvelope()`** rejects invalid baton values on incoming wire envelopes: non-string / empty `root_id`, non-canonical / not-in-allowed_from `origin`/`owner`, non-int / negative `hops`. Malformed values MUST NOT reach the model or get republished.

## 9 — Envelope-schema validation + vendoring

Vendor `bazfer/fleet-bus/schema/envelope.v1.schema.json` under `vendored/envelope.v1.schema.json` alongside `vendored/FLEET_BUS_SCHEMA_SHA256` and `vendored/FLEET_BUS_VERSION`. Dockerfile hash-check step per yugo v0.3e shape:

1. **Local sha256sum -c** against the recorded SHA. Catches drift within a single commit.
2. **Curl to `raw.githubusercontent.com/bazfer/fleet-bus/${version}/schema/envelope.v1.schema.json` + `cmp`.** Catches "vendored copy no longer matches the upstream commit it claims to pin."
3. **`FLEET_BUS_VERSION` MUST be a 40-char lowercase-hex SHA.** Both the pytest twin AND the Dockerfile RUN step enforce this. Mutation-tested control per yugo v0.3e R1 lesson (branch names, movable tags, short SHAs, uppercase all rejected).
4. **`.gitattributes` pins `vendored/*` to `eol=lf`.** Windows checkout with `core.autocrlf=true` silently rewrites LF→CRLF, invalidating the hash. Codex-found in yugo v0.3e R1.

Runtime schema validation using `ajv` on every inbound envelope before it reaches `handleInboundEnvelope`.

**SPEC §10 compat suite** (P2-14): consumer CI runs `test/compat/` against the pinned fleet-bus tag. Missing from v0; required per SPEC §10 and codex-adapter rollout step 7. Add to CI.

## 10 — Tool policy (v1: OFF)

**Bus turns run with `--allowed-tools=""`. No tools. Frame-in / tags-out only.**

Rationale:
- `from` is spoofable fleet-wide (SPEC §4 known limit, unfixed until subject-encoded sender lands — Deet INF-026 open item).
- Koi container holds Erika's memory files at `/app/memory`. Optimus holds JL's tenant data + Twilio-credentialed environment vars.
- `Read` alone lets a spoofed peer exfiltrate via a `<BUS to="<attacker>" payload="<file-contents>" />`.
- `WebFetch` is a direct exfil channel to arbitrary URLs.
- Bash + Write are catastrophic.

Tool-bearing bus turns are a **v2 item gated on sender authentication** (subject-encoded sender OR signed envelopes at coordinator). Until then, tools-off is the only safe posture.

Codex-adapter design does NOT explicitly address bus-turn tool scope (v0's cited "codex-adapter §11 precedent" was wrong — that section is auto-`in_reply_to` semantics, per P3-15). This design makes the tools-off decision on its own merits.

## 11 — Session isolation

The adapter process spawns claude subprocesses for bus turns only. User-message subprocesses are spawned by ingress processes with their own session-id namespaces. **No session id can be shared between bus turns and user turns**, by construction (different processes, different `--resume` caches).

Within bus turns:

- **Session namespace: `bus-<from>`** (per-peer collapsed). Follows Fernando's yugo v0.3b ruling after a Fable architect review. v0's `bus-<from>-<root_id>` degenerates on plain `text_message` envelopes (no baton) and grows unbounded on baton chains.
- **Turn cap: `BUS_HISTORY_MAX_TURNS=10`** default (env-overridable per bot). Older turns evicted from the resumable session's history.
- **Known limit: `from` is spoofable.** Per-peer isolation is hygiene (keeps conversation continuity between the same peer pair) but is not a security boundary until subject-encoded sender lands.

Ingress-side namespaces (for reference, out of this design's scope):
- Discord (Koi): `discord-<user_id>-<channel_id>`
- Telegram (Koi): `telegram-<chat_id>`
- Twilio (Optimus): `twilio-<from_number>`

Bus-turn session ids MUST NOT match any ingress-side namespace pattern — enforced by the `bus-` prefix.

## 12 — SPEC compliance matrix

| SPEC ref | Requirement | Design section (v1) | Test |
|---|---|---|---|
| Cl. 1 | Ignore unknown inbound fields | §9 ajv with `additionalProperties: true` | compat: extra-field envelope accepted |
| §2 | Required fields; `envelope_version === 1` | §4.5 + §9 | compat: each SPEC §5 reject code triggered |
| §2 | ≤ 1,044,480 bytes inbound + outbound | §6 outbound + §9 inbound | oversize envelope → `envelope_too_large` |
| §2 baton | Optional fields validated strict when present | §8 `validateInboundEnvelope` | malformed `hops`/`origin` rejected |
| §3 | Unknown `kind` not rejected | §4.5 (unknown kind reaches model or audits per policy — TBD) | unknown-kind envelope → handled or `unknown_kind` audit |
| §4 | `from` NFKC/lowercase + manifest allowlist | §3 manifestPath + `normalize_bot_name` | spoof-shaped `from` (fullwidth `ｖｅｃ`) rejected |
| §5 | Exact reject-code strings | §6.6 / §9 | string-equality assertions per code |
| §6 | Subscribe set (4 subjects, pre-FB-3) | §3.2 errata bullet 1 | deaf-bot probe per subject |
| §6 | Reply routing | **BLOCKED on Fernando ruling (P2-10)** | round-trip reply lands in peer transcript |
| §6 | `inboxPrefix` | §3.1 | request/reply against per-user perms succeeds |
| §7 | Frame: `authenticated="false"`, `from_claim`, `env_id` byte-identical, local `req_id` | §6.1 | frame snapshot tests incl. zero-width-in-env_id survival |
| §8 | JSONL audit; ts/dir/subject; env_id+req_id on in/out; adapter codes prefixed | §4 + §6 audit calls; mode 0600 at create | audit-line schema test; mutation drops a field → test fails |
| §9 | 30 s heartbeat, payload minimum keys | §5 | tap observes ≥2 beats, payload keys present |
| §10 | Consumer CI runs `test/compat/` vs pinned tag | §9 + rollout step 7 | CI job exists and is red on schema drift |

Empty cells eliminated. Every SPEC clause has a design section and a test.

## 13 — Rollout order

1. Land this design at v1 (post-review Ohm/Codex/Fable round 2).
2. **Fernando ruling on P2-10 reply routing** — must be recorded here before step 3.
3. Implement adapter process skeleton: NATS connect + subscribe + heartbeat + supervisor loop + own-status filter + hop-ceiling check + broadcast policy + dedup + injection frame render + audit. No spawn yet. Deploy to a **scratch claude-container bot** (not Koi, not Optimus). Prove deaf-bot probe passes and heartbeat is visible on tap.
4. Add `spawnClaudeForBusTurn` path with `--allowed-tools=""` and `--append-system-prompt` for the bus-turn prompt. Iterate the bus-turn system prompt against scratch bot: model reliably emits `<BUS>` tags-only, refuses via `<BUS>` not silence, observes self-address rejection. **Prompt-engineering iteration IS a rollout step.**
5. Add outbound `<BUS>` parser + publish. Prove round-trip against helm on scratch bot.
6. Add SPEC §10 compat suite to CI. Green on pinned fleet-bus tag.
7. Green on GATE suite end-to-end with helm + chis. Only then: enable on Koi, then Optimus.
8. `nats.js`-on-Bun runtime verification is an **exit criterion of step 3**, not an assumption.

Do NOT enable on Koi/Optimus before step 7. Both are Erika-facing and JL-facing respectively.

## 14 — Known risks

- **`claude --print` cold-start per bus turn.** Same profile as codex-container; measured tolerable. Watch bus-turn latency once deployed.
- **`from` spoofing.** Named in §10 and §11. Blocks tool-bearing bus turns until subject-encoded sender lands. Deet INF-026.
- **`nats.js` on Bun.** Not exercised in this fleet yet. Rollout step 3 exit criterion.
- **Adapter process crash storm.** If the supervisor loop somehow runs faster than `waitMs`, it could hot-spin. Guard: exponential backoff cap on the supervisor sleep + kill-switch env var.
- **`SIGTERM` graceful drain**: adapter handler MUST await `bus.drain()` before exit, or in-flight publishes get dropped. Explicit test.

## 15 — Not in scope for this design

- MCP-tool ergonomics (`bus_publish` / `bus_reply` as first-class tools instead of `<BUS>` text parsing). Deferred to v2.
- **Persistent Claude Code session flavor** (Luna, Deet, Kat). Different runtime, different lifecycle, separate document.
- Coordinator interaction / HITL policy enforcement (yugo v0.6+). This adapter is a wire participant; policy enforcement lives at coordinator.
- JetStream `.inbox` migration (FB-3). Per fleet-bus SPEC (not "§14" — see the v0 changelog on the ghost citation), the per-adapter flip to `.inbox` durable-consumer is a separate PR gated on coordinator forward-loop being live.
- Tool-bearing bus turns (see §10). Gated on subject-encoded sender authentication (v2).
- Audit log rotation. Deferred at the adapter level; add host-side `logrotate` stanza to Optimus/Koi deploys separately.

---

## Open questions the v1 review round should answer

1. **P2-10 reply routing** — Fernando's ruling; blocks rollout step 3.
2. **Unknown `kind` policy** (compliance matrix row §3): reach model or audit-drop? Codex-adapter reaches model; yugo audit-drops.
3. **Adapter-process language for Optimus.** v1 says Bun sidecar in the Python container for uniformity. Alternative: implement the adapter in Python (port `bus.py` shape directly) to avoid a language addition in a Python container. Trade-off: shared codebase with `bazfer/fleet-bus/src/bus.ts` vs one-less-runtime-in-container. Reviewer weigh in.
4. **Adapter shipping shape**: separate git repo (`bazfer/claude-container-bus-adapter`) OR co-located under `artifice-ia/claude-container/adapter/`? Prior art: codex-container ships bus adapter co-located. Follow that pattern.
