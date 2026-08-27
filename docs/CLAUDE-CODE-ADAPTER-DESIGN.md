# Claude-container fleet-bus adapter — design (v0 draft)

**Target repo:** [`artifice-ia/claude-container`](https://github.com/artifice-ia/claude-container)
**Implements:** [SPEC.md](../SPEC.md) for the Claude Code CLI runtime side.
**Consumers who will run this:** **Optimus** (JL DaaS, WhatsApp + Twilio ingress) and **Koi** (Erika familiar, Discord + Telegram ingress). Future bot-process claude-container deploys inherit this shape by default.
**Sibling:** [CODEX-ADAPTER-DESIGN.md](./CODEX-ADAPTER-DESIGN.md) — same wire, same trust boundary, different runtime.
**Status:** design draft v0, not yet implemented. Explicit REQUEST for adversarial review from Ohm + Codex + Fable before implementation touches a keyboard.

> **Scope note.** This document covers ONLY the **bot-process** flavor — claude-container bots that spawn `claude --print` per user message (Optimus, Koi, the shape codified in `artifice-ia/claude-container/discord-bot.ts` / `telegram-bot.ts`). The **persistent Claude Code session** flavor (Luna, Deet, Kat) is a distinct runtime with a different lifecycle and gets its own design document.

---

## 1 — The problem

Claude-container bots (Optimus, Koi) currently have no bus presence. Their ingress paths (`discord-bot.ts`, `telegram-bot.ts`, `webhook.ts` for Twilio) receive a user message, spawn `claude --print` as a one-shot subprocess with the message as stdin, capture stdout, deliver back to the ingress channel. Bus messages are invisible; the claude subprocess has no bus tools; no envelope this bot could receive from another bot in the fleet would reach the LLM.

We want the same shape the codex-container adapter (Vec's `bus.py`, v0.3) has: **received bus envelopes reach the model as a distinct injection frame; the model can publish outbound envelopes via a first-class tag pattern.** Applied to Claude Code CLI's actual runtime, not adapted from Python assumptions.

## 2 — Design summary

Bolt the bus onto the existing TypeScript ingress process (`discord-bot.ts` and siblings) — **not** onto the `claude` subprocess itself. Rationale:

1. **The subprocess is stateless per-message.** `claude --print` starts, reads stdin, writes stdout, exits. It has no long-lived event loop to attach an async NATS subscription to. The bus lifecycle needs a stable owner; the TS ingress process is that owner.
2. **The subprocess is short-lived (~seconds).** A bus envelope that arrives while no subprocess is running has to be buffered by *something*. The TS process is that buffer.
3. **Symmetry with codex-container.** Vec's bus.py runs in the Python bot.py process, which owns the codex app-server subprocess and its lifecycle. Same shape here: TS owns bus + owns the claude subprocesses it spawns.
4. **Trust boundary alignment.** Baton discipline (SPEC §4, per yugo `fleet_bus.py` HEAD and codex-container `bus.py` `derive_baton_fields`) requires the adapter to be the sole writer of baton fields. Model-authored output only ever contains `to`/`kind`/`payload`. The TS layer is where the sole-writer boundary lives; the subprocess sees only whatever the TS layer chooses to inject.

Two subsystems, each independently owned:

- **Inbound**: TS `nats` client (long-lived, one subscription per bot identity) subscribes `fleet.<bot>.request/result/status` + `fleet.broadcast.>` on ingress-process boot. On envelope: validate → render as a safely-escaped `<channel source="fleet-bus" ...>` injection frame → hand to a dedicated `spawnClaudeForBusTurn(frame)` path (NOT `spawnClaudeForUserMessage(text)`).

- **Outbound**: bus-triggered subprocess stdout is scanned for `<BUS to="..." kind="..." payload="{...}" />` tags via the same quote-aware attribute parser used in codex-container (regex is insufficient — payload attribute contains `>` in JSON). Validated envelope goes to NATS. Per-tag failure isolation (a malformed tag on one line doesn't fail the whole turn).

**No MCP sidecar needed for v1.** The `<BUS>` tag pattern proves out with the existing codex-adapter's assumptions; a proper MCP-tool interface is a v2 ergonomics upgrade, not a v1 correctness requirement. Deferring keeps this design shippable in isolation.

**Feature-flag off by default:** `FLEET_BUS_ENABLED=0` at `.env` shape. If disabled: zero NATS code paths execute. Ingress paths unchanged.

## 3 — Bus lifecycle (TS-side, per bot process)

Runs inside the ingress process (`discord-bot.ts`, `telegram-bot.ts`, `webhook.ts`) at boot, before message handlers register.

```typescript
if (env.FLEET_BUS_ENABLED === '1') {
  const bus = await FleetBus.connect({
    url:            env.FLEET_BUS_URL,        // nats://nats:4222
    user:           env.FLEET_BUS_USER,       // 'koi' | 'optimus'
    tokenFile:      env.FLEET_BUS_TOKEN_FILE, // path — read then discard
    manifestPath:   env.FLEET_MANIFEST_PATH,  // /vault/infra/fleet-manifest.yaml
    onEnvelope:     handleInboundEnvelope,    // async (env: Envelope) => void
    auditLogPath:   env.FLEET_BUS_AUDIT_LOG,
    inboxPrefix:    `_INBOX_${env.FLEET_BUS_USER}`,  // see reference_nats_inbox_prefix
    reconnect:      { maxAttempts: -1, waitMs: 2000 }, // -1 = infinite, per yugo E-1
  })
  process.once('SIGTERM', () => bus.drain())
  process.once('SIGINT',  () => bus.drain())
}
```

**Errata inherited from yugo v0.3a E-1 that this design MUST also honor:**
- Subscribe subjects are `.request` / `.result` / `.status` / `broadcast.>`, NOT `.inbox`. `.inbox` becomes a subject at FB-3 (JetStream migration); pre-FB-3 the per-adapter subscribe is direct.
- nats.js reconnect defaults abandon the connection permanently after N attempts. Must configure infinite reconnect.
- On the first failed permissions error against a subject, `nats.js` (like `nats-py`) invokes the error callback and returns without closing the connection. A bot that mis-subscribes is connected, heartbeating, and completely deaf — with every lifecycle test still green. **The v1 conformance test MUST publish a probe to `fleet.<self>.request` from a second process and assert `onEnvelope` fires, not just assert connection state.** This is the same trap the yugo v0.3a slice explicitly guards against.

## 4 — Inbound envelope → claude subprocess (bus-only, no Discord/Telegram tag processors)

`handleInboundEnvelope(env)` — called by the FleetBus with a schema-validated envelope. Steps:

1. **Per-envelope dedup.** SQLite at `env.YUGO_DEDUP_STORE_PATH` (default `/var/lib/claude-container/<bot>-dedup.sqlite`, retention ≥ 8d to survive the JetStream stream's 7d max_age per SPEC §14). Second delivery of the same envelope-id is a silent no-op with `claude_adapter_duplicate_delivery` audit.

2. **Render injection frame.** Build a `<channel source="fleet-bus" env_id="..." from="..." kind="..." req_id="..." ts="..." [root_id="..." origin="..." owner="..." hops="..."]>` XML-tagged text block. Payload content sits inside the tag body, XML-escaped. Identifier fields (`env_id`, `req_id`, `root_id`) MUST NOT get zero-width stripping — audit correlation depends on them being byte-identical (same rule codex-adapter §5 codified after a v3 Codex finding).

3. **Serialize per bot.** Use a single lock (`Mutex` or async semaphore) shared with the user-message ingress path. A bus envelope + a Discord message arriving simultaneously should serialize; both should not spawn concurrent `claude` subprocesses for this bot. Codex-container solves this via `asyncio.Lock`; the TS equivalent is a fair async lock.

4. **Spawn dedicated bus subprocess.** `claude --print --input-format stream-json` (or the current `spawnClaudeForUserMessage` shape) with:
    - **`SESSION_ID` distinct from user-message sessions.** The `--session` flag / `--resume` path used for user Discord messages MUST NOT be shared with the bus-triggered turn. Same class of vulnerability codex-container just closed at PR #3 R1: sharing session state between an untrusted bus peer and a human user leaks history in both directions. The bus turn gets its own session identifier, ideally per-peer (`bus-<from>-<root_id>`) so bus conversations with vec are isolated from bus conversations with helm.
    - **Stdin = the injection frame ONLY.** No Discord history, no user prompt. The frame is the entire input.
    - **Stdout captured, stderr logged separately for the audit trail.**

5. **Discord/bus isolation.** The claude subprocess run on a bus turn is bus-only in output: `<BUS>` tags in stdout are published; any prose or other tags (`<CRON>`, image attachments, `<REPLY>`) are dropped with `claude_adapter_prose_from_bus_turn_discarded` audit. Codex-adapter §11 codifies this; same rule here for the same reason (bus peer must not be able to inject anything into the Discord surface).

## 5 — Frame escaping

Identity fields (`env_id`, `req_id`, `root_id`) rendered as-is, no transformation — audit-log correlation depends on byte-identity.

Human-visible fields (`from`, `kind`, payload body):
- Zero-width character stripping (`[​-‏‪-‮⁠-⁯﻿]`) — same regex codex-adapter uses.
- XML attribute escaping for the five entities we emit (`& < > " '`), NOT arbitrary numeric entities.

Frame construction wrapped in try/catch; failure emits `claude_adapter_envelope_frame_too_large` (or `_render_failed`) audit and drops the envelope, does NOT publish an unrendered version.

## 6 — Outbound `<BUS>` tag processing

Same shape as codex-container `bus.py::process_bus_tags` v0.3.1 (post-baton-vuln-fix). Explicit constraints, since these are the ones the codex-container PR #3 iteration burned three review rounds on:

1. **Quote-aware attribute parser.** Regex on `<BUS[^>]*>` is insufficient — `payload` attribute holds arbitrary JSON, which contains `>`. Ports the codex-adapter's stateful parser.
2. **`\<c>` escape preservation.** `\"` unescapes only when the outer quote is `"` (and analogously for `'`). Everything else passes through — JSON `\n`, `\t`, `\"` inside `'...'` attributes survive to `JSON.parse`.
3. **`JSON.parse` (or equivalent) called with reviver rejecting `NaN`/`Infinity`.** These aren't JSON-standard; codex-adapter added `parse_constant` guard in Python + `allow_nan=False` on outbound. TypeScript-side: use `strict` JSON reviver.
4. **Broadcast subject regex uses `fullmatch` shape** with non-empty dot-separated tokens: `[a-z0-9_-]+(\.[a-z0-9_-]+)*`. Direct `to` field validated via `normalize_bot_name` (`^[a-z0-9_-]+$`, must be in the fleet manifest's `bot_names`) BEFORE forming `fleet.<to>.request` — prevents `fleet..request` or `fleet.luna extra.request`.
5. **Malformed tag ⇒ stripped from output AND audited.** The tag span is recorded even when parse fails, so if this turn produces any surviving prose (which it shouldn't for bus-only turns, but see §7), the tag doesn't leak as raw text.
6. **All adapter-invented audit reasons prefixed `claude_adapter_`.** SPEC §5 reject codes (unprefixed) reserved for the standardized taxonomy; adapter-side codes stay in a distinct namespace.

## 7 — Baton discipline (INHERITED FROM YUGO REFERENCE)

**This section is not up for debate at design review. It's the class-fix from codex-container PR #3 R3 that closed a 3-round vuln iteration. Replicate it, don't redesign it.**

- **Baton fields have exactly one source: the validated inbound envelope.** Model output can carry `to`, `kind`, `payload` on `<BUS>` tags. Any baton key (`root_id`, `origin`, `owner`, `hops`) appearing in a tag ⇒ REJECT with `claude_adapter_baton_field_in_tag`, do NOT silently drop the offending key and keep the rest of the tag.
- **Baton fields have exactly one writer: `deriveBatonFields()`.** Single function, one code path, used by every outbound-envelope construction site. Signature (in TS): `deriveBatonFields({envelopeId, identityName, recipient, kind, inbound})` where `inbound` is either the wire envelope this turn was triggered by OR `null` for originating turns (Discord/Telegram/webhook-triggered where the bot chose to `<BUS>`-out on its own).
- **On originating turns**: `root_id = envelopeId`, `origin = identityName`, `owner = identityName`, `hops = 0`.
- **On response/handoff turns**: `root_id = inbound.root_id`, `origin = inbound.origin`, `owner = inbound.owner`, `hops = inbound.hops + 1`.
- **On `kind === 'baton.handoff'`**: derive new `owner = normalize_bot_name(recipient)`. Refuse handoffs whose recipient is invalid, missing, or broadcast (per PR #3 R3 finding).
- **Post-derivation hop bound check**: if `derivedHops >= 16`, throw `BatonHopsExhausted` before publish. Peers reject envelopes with `hops >= 16` at their own pre-model check; publishing a `hops=16` envelope produces "reply disappears silently" for the sender. Same trap PR #3 R2 closed.
- **`validate_envelope()` (inbound side)** rejects invalid baton values on incoming wire envelopes: non-string / empty `root_id`, non-canonical / not-in-`allowed_from` `origin`/`owner`, non-int / negative `hops`. Malformed values MUST NOT reach the model or get republished.
- **Loop backstops layered, all retained, not either/or**: (a) no auto-reply to an envelope carrying `in_reply_to`; (b) `<BUS>` tag may not address self; (c) hops warning at 8 to `origin`; (d) hops refusal at 16. Yugo runbook confirms all four fire together per Fernando's ruling.

## 8 — Envelope-schema validation

Vendor `bazfer/fleet-bus/schema/envelope.v1.schema.json` under `vendored/envelope.v1.schema.json` alongside `vendored/FLEET_BUS_SCHEMA_SHA256` and `vendored/FLEET_BUS_VERSION`. Dockerfile hash-check step per yugo v0.3e shape:

1. **Local sha256sum -c** on the vendored file against the recorded SHA. Catches drift within a single commit.
2. **Curl to `raw.githubusercontent.com/bazfer/fleet-bus/${version}/schema/envelope.v1.schema.json` + `cmp`.** Catches "vendored copy no longer matches the upstream commit it claims to pin."
3. **`FLEET_BUS_VERSION` MUST be a 40-char lowercase-hex SHA.** Both the pytest twin AND the Dockerfile RUN step enforce this. Mutable refs (branch names, movable tags) defeat the reproducibility claim. This is a mutation-tested control per yugo v0.3e R1 lesson.
4. **`.gitattributes` pins `vendored/*` to `eol=lf`.** Windows checkout with `core.autocrlf=true` silently rewrites LF→CRLF, invalidating the hash. Codex found this in yugo v0.3e R1.

Runtime schema validation (using `ajv` or equivalent) applied to every inbound envelope BEFORE it reaches `handleInboundEnvelope`. Failed validation → `claude_adapter_schema_invalid` audit, envelope discarded.

## 9 — Session isolation across ingress paths

A single bot process (say, Koi) has FOUR concurrent envelope sources:
1. Discord DMs
2. Telegram DMs (or channel messages, per bot)
3. Twilio WhatsApp webhooks (Optimus)
4. Fleet-bus envelopes

**Each source gets its own claude session-id namespace.** No message from source (1) may resume a session established by source (4), or vice versa. This is the direct application of the codex-container PR #3 R1 finding to a bot that has more than two ingress paths.

Suggested namespace shape:
- Discord: `discord-<user_id>-<channel_id>`
- Telegram: `telegram-<chat_id>`
- Twilio: `twilio-<from_number>`
- Bus: `bus-<from>-<root_id>` (per-peer per-conversation-tree)

`--resume` calls filtered by prefix; no cross-prefix resume allowed.

## 10 — Env vars, deps

```
FLEET_BUS_ENABLED=0                              # default off
FLEET_BUS_URL=nats://nats:4222                   # container network endpoint
FLEET_BUS_USER=koi                               # bot identity, matches nats.conf
FLEET_BUS_TOKEN_FILE=/root/.claude/fleet-bus-token-koi
FLEET_MANIFEST_PATH=/vault/infra/fleet-manifest.yaml
FLEET_BUS_AUDIT_LOG=/root/.claude/fleet-bus-log.jsonl
YUGO_DEDUP_STORE_PATH=/var/lib/claude-container/koi-dedup.sqlite
```

Dependencies:
- `nats.js` (v2.x — the one Vec's plugin also uses; single source of truth for the wire client)
- `ajv` (or `zod`) for schema validation
- `better-sqlite3` for dedup store (Bun has `bun:sqlite` built-in — prefer it if the ingress runs on Bun; discord-bot.ts already uses Bun)

## 11 — SPEC compliance matrix

*(Ohm-style verify grid; empty in v0, to be filled in during review iterations. Every SPEC clause maps to a code location and a test.)*

## 12 — Rollout order

1. Land this design at v1 (post-review with Ohm/Codex/Fable).
2. Implement inbound-only path first (subscribe + validate + inject + audit). No outbound `<BUS>` yet. Deploy to a scratch claude-container bot (not Koi or Optimus). Prove the connection lifecycle + subscribe permissions work as designed against the same trap yugo v0.3a fell into.
3. Add outbound `<BUS>` parser + publish (bus-turn output only). Deploy to scratch.
4. Add Discord-triggered `<BUS>` tag path (a message from a human causes the bot to `<BUS>` at another bot). Deploy to scratch.
5. Once GATE test passes end-to-end on scratch bot with helm + chis in the fleet: enable on Koi and Optimus.

Do NOT enable on Koi/Optimus before the scratch prove-out. Both are Erika-facing and JL-facing respectively.

## 13 — Known risks

- **`claude --print` cold-start cost per bus turn.** Each envelope spawns a fresh subprocess. Codex-container has the same cost profile; measured to be tolerable. Watch bus-turn latency once deployed.
- **Session store cardinality.** Per-peer per-root_id session IDs could grow large in a fleet with active baton chains. `--resume` cache TTL / GC needed if this becomes a memory issue.
- **Ambient tools.** Claude Code CLI's tool surface (Bash, WebFetch, Read, Write, etc.) is available INSIDE the bus-triggered turn. That means a bus peer effectively has read/write access to the bot's filesystem within whatever `--allowed-tools` scope is configured. Bus turns SHOULD run under a tighter `--allowed-tools` list than user-message turns — spec that scope explicitly in v1 rather than defaulting to whatever the user-message path uses.

## 14 — Not in scope for this design

- MCP-tool ergonomics (`bus_publish` / `bus_reply` as first-class tools instead of `<BUS>` text parsing). Deferred to v2.
- **Persistent Claude Code session flavor** (Luna, Deet, Kat). Different runtime, different lifecycle, separate document. This design covers only the bot-process flavor.
- Coordinator interaction / HITL policy enforcement (yugo v0.6+). This adapter is a wire participant; policy enforcement lives at coordinator.
- JetStream `.inbox` migration (FB-3). Per SPEC §14, the per-adapter flip to `.inbox` durable-consumer is a separate PR gated on coordinator forward-loop being live.

---

## Open questions for review

1. **Session isolation across bus peers.** Is `bus-<from>-<root_id>` the right shape, or does per-peer-per-day / per-peer-collapsed make more sense? Trade-off: memory usage vs conversation continuity between the same peer pair.
2. **Bus-turn `--allowed-tools` scope.** What's the minimum tool set a bus-triggered turn should have? Read-only Read/Grep/Glob at most? Or fully sandboxed with no tools? Codex-container decided no tools for bus turns (per §11 codex-adapter). Should we mirror that or allow reads?
3. **Twilio webhook flavor.** Optimus has an HTTPS ingress (`webhook.ts`) that hands off to the claude subprocess. Same adapter shape works, but the webhook process's lifecycle is per-request Cloudflare — where does the long-lived NATS subscription live? Options: separate always-on subscriber process that queues envelopes for the webhook to drain; or the webhook stays cold and Koi's Discord-side bot subscribes on Optimus's behalf. Second option smells wrong (cross-tenant coupling). First is cleaner but adds a process.
4. **Audit log rotation.** `fleet-bus-log.jsonl` grows unbounded. yugo v0.3 doesn't rotate; codex-container doesn't rotate. Should this design specify rotation, or continue to defer?
5. **`kind` semantics reconciliation.** yugo §7 uses `.request` + `in_reply_to` for replies; fleet-bus SPEC §6 says replies route to `.result`. Deet's runbook flags this as upstream reconciliation work. This adapter follows whichever wins on the SPEC side; noting the ambiguity here so the design reviewer flags if a decision is needed before implementation.
