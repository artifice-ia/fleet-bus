# Codex-container fleet-bus adapter — design (v3)

**Target repo:** [`artifice-ia/codex-container`](https://github.com/artifice-ia/codex-container)
**Implements:** [SPEC.md](../SPEC.md) for the Python side
**Consumers who will run this:** Vec, Ohm, Myc, Helm
**Status:** design draft, not yet implemented

**Changes since v2** (in response to Ohm's DeetGates review + Codex re-review of PR #1):

Ohm v2 (5 findings):
- Replies now route to `fleet.<to>.result`, not `.request` (SPEC §6)
- `BUS_TAG_RE` replaced with quote-aware attribute parser; naive regex broke on payloads containing `>`
- Baton additive fields (`root_id`, `origin`, `owner`, `hops`) preserved on outbound and passed through on inbound — per SPEC §14 "not in scope for baton semantics" but must not be dropped
- Bus-triggered turns run ONLY the bus tag processor. `<CRON>`/`<CRON_REMOVE>` and other Discord-side capabilities are NOT exposed to bus turns; non-bus tags in bus-turn output are audited and discarded
- Lifecycle switched from `bot.loop.create_task(...)` before `bot.run()` (unsafe on modern discord.py) to `setup_hook()` (the proper async startup hook), with task-handle retention and reconnect-idempotency

Codex re-review (5 P2):
- Frame construction wrapped in its own try/except emitting `envelope_frame_too_large` audit event so `_escape_attr` raising doesn't silently abort
- Zero-width stripping applied ONLY to human-readable content fields (`from_claim`, `kind` visible), NOT to identifier fields (`id`, `env_id`, `req_id`, `ts`) — SPEC §7 requires `env_id` unchanged for audit-log correlation
- `_bus_lifecycle` disposes the closed bus instance in a `finally` block before reconnecting; no heartbeat/subscription leak on flap
- All inbound audit events (including model timeout, model error, discarded prose) carry `subject` — passed through the callback chain
- Outbound audits carry `req_id` — `_process_bus_tags` accepts `req_id` and includes it in every publish audit event

## 1 — The problem

Codex containers currently have no bus presence. `bot.py` handles Discord ingress → forwards to a local `codex app-server` over WebSocket → sends the reply back to Discord. Bus messages are invisible; bus publishes require ad-hoc bun scripts run via `functions.exec` that don't persist across turns.

We want the same shape the Claude Code plugin has: **received bus envelopes appear in the codex session as a distinct injection frame; the model can publish outbound envelopes via a first-class tag pattern.**

## 2 — Design summary

Bolt the bus onto the existing `bot.py` — no new services, no separate daemon. Two subsystems, each independently owned:

1. **Inbound path**: Python `nats-py` client (background task, separate lifecycle from Codex) subscribes to `fleet.<bot>.request/result/status` and `fleet.broadcast.>` on startup. On envelope: validate → format as safely-escaped `<channel source="fleet-bus" ...>` text → hand to codex via a **dedicated bus-only** call path (not the Discord `_ask()`). Reuses `_lock` so bus and Discord traffic serialize naturally.

2. **Outbound path**: model emits `<BUS to="..." kind="..." payload="{...}" />` tags in its replies; a new `_process_bus_tags()` async processor extracts them using a **quote-aware parser** (regex won't do), validates against SPEC.md §5, and publishes to NATS. Per-tag failure isolation.

**No `mcp.notification` equivalent needed** — codex accepts arbitrary text as `turn/start` input, and that IS the injection channel.

## 3 — Bus lifecycle (setup_hook, separate from Codex)

**Feature-flag off by default.**

```python
FLEET_BUS_ENABLED = os.environ.get("FLEET_BUS_ENABLED", "0") == "1"
```

If disabled: zero NATS code executes. Discord path unaffected.

**Lifecycle via `setup_hook()`** — the discord.py-supported async startup hook that runs after login but before event dispatch. Old design's `bot.loop.create_task(...)` before `bot.run()` is unsafe on modern discord.py (the loop isn't running yet).

```python
class ArtificeBot(commands.Bot):
    async def setup_hook(self) -> None:
        # Runs after login, before event dispatch. This is where discord.py
        # expects long-lived background tasks to be scheduled.
        self._bus_task: asyncio.Task | None = None
        if FLEET_BUS_ENABLED:
            self._bus_task = self.loop.create_task(
                _bus_lifecycle(),
                name="fleet-bus-lifecycle",
            )

    async def close(self) -> None:
        # Called on graceful shutdown. Cancel + await bus task, then drain.
        if self._bus_task and not self._bus_task.done():
            self._bus_task.cancel()
            try:
                await self._bus_task
            except (asyncio.CancelledError, Exception):
                pass
        if _bus is not None:
            await _bus.disconnect()
        await super().close()

async def _bus_lifecycle() -> None:
    """Owns the FleetBus lifetime. Reconnects with backoff. Cleans up on
    every retry so we don't leak heartbeat tasks or subscriptions
    (Codex re-review finding #3)."""
    global _bus
    backoff = 1
    while True:
        current = None
        try:
            current = FleetBus(...config...)
            _bus = current
            await current.connect()  # subscribes + starts heartbeat
            backoff = 1
            await current.wait_closed()
            print("[bus] connection lost, will reconnect")
        except asyncio.CancelledError:
            # Graceful shutdown path — dispose and exit.
            if current is not None:
                await current.disconnect()
            raise
        except Exception as e:
            print(f"[bus] connect/run failed: {e}")
        finally:
            # Always dispose the current instance before looping.
            # This is what stops heartbeat tasks and subscriptions from
            # accumulating across reconnects.
            if current is not None:
                try:
                    await current.disconnect()
                except Exception:
                    pass
            _bus = None
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 60)
```

**Codex reinit does NOT touch the bus.** `_init_codex()` continues to only manage the Codex WebSocket. If Codex WS dies, `_bus` remains unaffected. If NATS dies, `_init_codex()` doesn't run.

## 4 — Inbound envelope → codex turn (bus-only, no Discord tag processors)

Bus-triggered turns use `_ask_bus()` (dedicated code path, no Discord wiring) and run **only the bus tag processor**. `<CRON>` and other Discord-side capabilities MUST NOT be exposed to bus turns — unverified bus input inducing persistent scheduler state is exactly the kind of privilege leak the injection-frame separation exists to prevent (Ohm v2 finding #4).

```python
async def _on_bus_envelope(subject: str, envelope: dict, req_id: str) -> None:
    """Invoked by FleetBus for each validated envelope. `subject` is threaded
    through so every audit event includes it per SPEC §8 (Codex finding #4)."""

    try:
        frame = _build_injection_frame(envelope, req_id)
    except ValueError as e:
        # Frame construction failed (e.g. attribute overrun >1024 chars).
        # Codex finding #1: this used to raise before the try, silently
        # aborting without an audit. Now handled explicitly.
        _bus.audit({
            "dir": "drop", "subject": subject,
            "envelope_id": envelope.get("id"), "req_id": req_id,
            "reason": "envelope_frame_too_large" if "1024" in str(e) else "envelope_frame_invalid",
            "error": str(e),
        })
        return

    async with _lock:
        try:
            raw_reply = await _ask_bus(frame)
        except asyncio.TimeoutError:
            _bus.audit({"dir": "drop", "subject": subject,
                        "envelope_id": envelope["id"], "req_id": req_id,
                        "reason": "model_timeout"})
            return
        except Exception as e:
            _bus.audit({"dir": "drop", "subject": subject,
                        "envelope_id": envelope["id"], "req_id": req_id,
                        "reason": "model_error", "error": str(e)})
            return

    # BUS-ONLY: only _process_bus_tags runs. NOT _process_cron_tags.
    # Bus input must not trigger scheduler changes (Ohm v2 finding #4).
    cleaned, bus_notes = await _process_bus_tags(
        raw_reply,
        req_id=req_id,                # per Codex finding #5, propagate req_id
        in_reply_to=envelope["id"],   # per §11, auto-set
        subject_context=subject,      # for audit events
    )

    # Non-tag prose from a bus turn is discarded. Ohm v1 finding #2 still holds.
    if cleaned.strip():
        _bus.audit({"dir": "drop", "subject": subject,
                    "envelope_id": envelope["id"], "req_id": req_id,
                    "reason": "prose_from_bus_turn_discarded",
                    "prose_length": len(cleaned)})

    # Note also: if the model emitted a <CRON> in a bus turn, it survives
    # into `cleaned` (since we don't run _process_cron_tags on bus turns),
    # then is discarded via the prose-discard rule above and audited under
    # `prose_from_bus_turn_discarded`. No scheduler change occurs.

async def _ask_bus(frame: str) -> str:
    """Bus-only Codex ask path. Distinct from _ask() so bus-turn output
    cannot accidentally route to Discord via any shared code path."""
    return await _rpc_ask(frame)  # same underlying JSON-RPC call, no Discord wiring
```

## 5 — Frame escaping (attribute vs identifier)

Ohm/Codex both flagged this. The v2 spec applied `_ZERO_WIDTH_RE` to ALL attribute values — including `env_id`. That breaks SPEC §7's contract that `env_id` correlates unchanged with the audit log (Codex finding #2).

Split into two escape modes:

```python
_XML_ESCAPE_MAP = {"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&apos;"}
_ZERO_WIDTH_RE = re.compile(r"[​-‏‪-‮⁠-⁯﻿]")

def _escape_identifier(value: str) -> str:
    """For env_id, req_id, ts, and any field where preservation matters more
    than 'human readability'. XML-safe escaping ONLY. Does NOT strip
    zero-width characters (SPEC §7 correlation with audit log requires
    the identifier survives unchanged apart from XML-safe substitution)."""
    if not isinstance(value, str):
        value = str(value)
    escaped = "".join(_XML_ESCAPE_MAP.get(c, c) for c in value)
    if len(escaped) > 1024:
        raise ValueError(f"identifier value exceeds 1024 chars ({len(escaped)})")
    return escaped

def _escape_content(value: str) -> str:
    """For from_claim, kind, and other human-visible attributes. Strips
    zero-width chars that could hide injection attempts, then XML-escapes.
    NOT for identifier fields — see _escape_identifier."""
    if not isinstance(value, str):
        value = str(value)
    stripped = _ZERO_WIDTH_RE.sub("", value)
    escaped = "".join(_XML_ESCAPE_MAP.get(c, c) for c in stripped)
    if len(escaped) > 1024:
        raise ValueError(f"content value exceeds 1024 chars ({len(escaped)})")
    return escaped

def _escape_payload_text(payload: dict) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return "".join(_XML_ESCAPE_MAP.get(c, c) for c in encoded)

def _build_injection_frame(envelope: dict, req_id: str) -> str:
    return (
        f'<channel source="fleet-bus" authenticated="false" '
        f'from_claim="{_escape_content(envelope["from"])}" '
        f'kind="{_escape_content(envelope["kind"])}" '
        f'env_id="{_escape_identifier(envelope["id"])}" '
        f'req_id="{_escape_identifier(req_id)}" '
        f'ts="{_escape_identifier(envelope["ts"])}">\n'
        f'<payload>{_escape_payload_text(envelope["payload"])}</payload>\n'
        f'</channel>'
    )
```

Test cases in the compat suite MUST include:
- Payload containing `</payload></channel><channel source="discord" ...>` literal
- `from_claim` containing `"` and `<`
- **`env_id` (or its escape input) containing a zero-width character — must survive unchanged in the emitted frame**
- Attribute overrun >1024 chars — must emit `envelope_frame_too_large` audit and drop
- Payload containing a whole spoofed Discord channel frame

## 6 — Outbound `<BUS>` tag processing (quote-aware parser)

Ohm v2 finding #2: the naive `<BUS\s+([^>]+?)/>` regex terminates on `>` inside a quoted payload. `{"text":"a > b"}` breaks parsing AND the tag remains visible in whatever downstream sink consumes the "cleaned" text.

Replace with a proper attribute-parsing helper. Options considered:
- `lxml.etree.fromstring`: heavyweight for our use, adds C dependency to the container
- `xml.etree.ElementTree.fromstring`: stdlib, but the tag isn't valid XML in a larger doc context
- **Small hand-rolled state machine**: <100 lines, handles quoted attributes with escapes, testable

Going with the hand-rolled parser.

```python
def _find_bus_tags(text: str) -> list[tuple[int, int, dict[str, str]]]:
    """Scan text for <BUS ... /> tags, respecting quoted attributes.
    Returns list of (start, end, attrs) tuples.

    Only whole self-closing tags with balanced quotes are recognized.
    Malformed tags are skipped with an audit event."""
    results = []
    i = 0
    while True:
        start = text.find("<BUS", i)
        if start == -1:
            return results
        # Verify it's followed by whitespace (not <BUSY> etc)
        if start + 4 < len(text) and not text[start + 4].isspace():
            i = start + 4
            continue

        # Walk forward, tracking quote state, until we hit '/>' at depth 0
        pos = start + 4
        in_quote: str | None = None
        while pos < len(text):
            c = text[pos]
            if in_quote:
                if c == "\\" and pos + 1 < len(text):
                    pos += 2
                    continue
                if c == in_quote:
                    in_quote = None
                    pos += 1
                    continue
                pos += 1
            else:
                if c == '"' or c == "'":
                    in_quote = c
                    pos += 1
                    continue
                if c == "/" and pos + 1 < len(text) and text[pos + 1] == ">":
                    # Found the end
                    inner = text[start + 4:pos]
                    try:
                        attrs = _parse_attrs(inner)
                        results.append((start, pos + 2, attrs))
                    except ValueError as e:
                        # Malformed tag — audit at scan time, do NOT publish
                        _bus.audit({"dir": "drop", "reason": "outbound_tag_malformed",
                                    "error": str(e), "raw": text[start:pos + 2][:200]})
                    i = pos + 2
                    break
                if c == "<":
                    # New tag started without our tag closing — malformed
                    _bus.audit({"dir": "drop", "reason": "outbound_tag_unclosed",
                                "raw": text[start:start + 200]})
                    i = start + 4
                    break
                pos += 1
        else:
            # Ran off end of text without closing
            _bus.audit({"dir": "drop", "reason": "outbound_tag_unclosed",
                        "raw": text[start:start + 200]})
            return results

def _parse_attrs(inner: str) -> dict[str, str]:
    """Parse `key="value" key='value'` pairs from tag inner. Handles both
    quote styles, backslash escapes inside quotes, XML entity references
    (&amp; &lt; &gt; &quot; &apos;)."""
    attrs = {}
    pos = 0
    n = len(inner)
    while pos < n:
        while pos < n and inner[pos].isspace():
            pos += 1
        if pos >= n:
            break
        # Read key
        key_start = pos
        while pos < n and (inner[pos].isalnum() or inner[pos] == "_"):
            pos += 1
        if pos == key_start:
            raise ValueError(f"expected attribute key at position {pos}")
        key = inner[key_start:pos]
        if pos >= n or inner[pos] != "=":
            raise ValueError(f"expected '=' after attribute key {key!r}")
        pos += 1
        if pos >= n or inner[pos] not in ('"', "'"):
            raise ValueError(f"expected quoted value for {key!r}")
        quote = inner[pos]
        pos += 1
        val_start = pos
        val_chars = []
        while pos < n and inner[pos] != quote:
            if inner[pos] == "\\" and pos + 1 < n:
                val_chars.append(inner[pos + 1])
                pos += 2
            else:
                val_chars.append(inner[pos])
                pos += 1
        if pos >= n:
            raise ValueError(f"unclosed quote for {key!r}")
        pos += 1  # skip closing quote
        raw_value = "".join(val_chars)
        # Unescape XML entities
        for entity, char in (("&amp;", "&"), ("&lt;", "<"), ("&gt;", ">"),
                             ("&quot;", '"'), ("&apos;", "'")):
            raw_value = raw_value.replace(entity, char)
        attrs[key] = raw_value
    return attrs
```

Then in the processor:

```python
BATON_FIELDS = {"root_id", "origin", "owner", "hops"}

def _pick_publish_subject(env: dict) -> str:
    """SPEC §6: replies go to fleet.<to>.result, requests go to fleet.<to>.request.
    Broadcasts (no `to`) go to fleet.broadcast.<kind>. Fix for Ohm v2 #1."""
    if env.get("to") is None:
        return f"fleet.broadcast.{env['kind']}"
    if env.get("in_reply_to"):
        return f"fleet.{env['to']}.result"
    return f"fleet.{env['to']}.request"

async def _process_bus_tags(
    text: str,
    req_id: str | None = None,
    in_reply_to: str | None = None,
    subject_context: str | None = None,
) -> tuple[str, list[str]]:
    """Extract <BUS ... /> tags, publish each async with per-tag failure isolation.
    Returns (text-with-tags-removed, per-tag human-readable notes)."""
    tags = _find_bus_tags(text)
    if not tags:
        return text, []

    notes = []

    for start, end, attrs in tags:
        try:
            payload = json.loads(attrs.get("payload", "{}"))
        except json.JSONDecodeError as e:
            notes.append(f"[bus tag ignored: payload JSON invalid: {e}]")
            _bus.audit({"dir": "drop", "subject": subject_context,
                        "reason": "outbound_tag_payload_json_invalid",
                        "raw": attrs.get("payload", "")[:200]})
            continue

        env = {
            "envelope_version": 1,
            "id": str(uuid.uuid4()),
            "from": IDENTITY_NAME,
            "to": attrs.get("to"),
            "kind": attrs.get("kind", "text_message"),
            "ts": datetime.now(timezone.utc).isoformat(),
            "payload": payload,
        }

        # in_reply_to: attribute wins, then trigger-envelope default
        if "in_reply_to" in attrs:
            env["in_reply_to"] = attrs["in_reply_to"]
        elif in_reply_to:
            env["in_reply_to"] = in_reply_to

        # Baton additive-field pass-through (Ohm v2 #3). The model can set
        # these on the outbound tag; we preserve them without imposing
        # semantics (SPEC §14 — baton lifecycle is not in this adapter's scope).
        for baton_field in BATON_FIELDS:
            if baton_field in attrs:
                if baton_field == "hops":
                    try:
                        env["hops"] = int(attrs["hops"])
                    except ValueError:
                        notes.append(f"[bus tag ignored: hops not integer]")
                        _bus.audit({"dir": "drop", "subject": subject_context,
                                    "reason": "outbound_baton_hops_not_integer"})
                        continue
                else:
                    env[baton_field] = attrs[baton_field]

        validation = _bus.validate_outbound(env)  # applies SPEC §5 reject codes
        if not validation["ok"]:
            notes.append(f"[bus tag ignored: {validation['error']}]")
            _bus.audit({"dir": "drop", "subject": subject_context,
                        "reason": validation["error"], "envelope_id": env["id"],
                        "req_id": req_id})
            continue

        subject = _pick_publish_subject(env)

        try:
            await _bus.publish(subject, env)
            # Codex finding #5: outbound audits MUST include req_id (SPEC §8)
            _bus.audit({"dir": "out", "subject": subject,
                        "envelope_id": env["id"], "req_id": req_id})
            notes.append(
                f"→ bus: {env['from']}→{env.get('to') or 'broadcast'} "
                f"{env['kind']} ({env['id'][:8]}) via {subject}"
            )
        except Exception as e:
            notes.append(f"[bus publish failed: {e}]")
            _bus.audit({"dir": "drop", "subject": subject,
                        "reason": "publish_error",
                        "envelope_id": env["id"], "req_id": req_id,
                        "error": str(e)})
            # Do NOT re-raise: one failed tag must not abort the batch.

    # Rebuild text without the tags
    keep = []
    prev = 0
    for start, end, _ in tags:
        keep.append(text[prev:start])
        prev = end
    keep.append(text[prev:])
    text_cleaned = "".join(keep).strip()
    return text_cleaned, notes
```

## 7 — SPEC compliance matrix

| SPEC.md ref | Requirement | Codex adapter implementation |
| --- | --- | --- |
| §1 clause 1 | Consumers MUST ignore unknown fields | Envelope validator uses `additionalProperties: true`; adapter reads only §2 documented + baton additive fields; silently accepts extras |
| §2 | envelope_version = 1 required | Reject with `unsupported_envelope_version` if absent or not 1 |
| §2 | id, from, kind, ts, payload required | Reject with `invalid_id` / `from_claim_rejected` / `invalid_kind` / `invalid_ts` / `missing_payload` |
| §2 | to optional, string or null | Reject with `invalid_to` if present and not string/null |
| §2 | in_reply_to optional, string | Reject with `invalid_in_reply_to` if present and not string |
| §2 | Envelope ≤ 1,044,480 bytes encoded | Reject with `envelope_too_large`; enforce before publish (outbound) and on receive (inbound) |
| §2 baton additive | root_id/origin/owner/hops accepted, not required | Preserved verbatim on inbound; passed through on outbound via BATON_FIELDS |
| §4 | from normalized (NFKC, lowercase, `[a-z0-9_-]+`) | Apply normalization then match against manifest allowlist; reject with `from_claim_rejected` |
| §4 | Manifest allowlist load | Load `~/vault/infra/fleet-manifest.yaml` at connect; refuse to start if missing/empty |
| §5 | Reject codes exact spelling | Use the exact strings from §5 in every audit event `reason` field |
| §6 | Subscriptions | `fleet.<bot>.request`, `fleet.<bot>.result`, `fleet.<bot>.status`, `fleet.broadcast.>` (multi-level wildcard) |
| §6 | Reply routing | Replies (envelope with `in_reply_to`) publish to `fleet.<to>.result`, not `.request` — see `_pick_publish_subject` |
| §6 | inboxPrefix required | Pass `inbox_prefix=f"_INBOX_{bot_name}"` to `nats.connect(...)` |
| §7 | Injection frame attributes | See §5 of this doc; identifier fields use `_escape_identifier` (no zero-width strip), content fields use `_escape_content` |
| §7 | authenticated="false" required | Hardcoded literal, never variable |
| §8 | Audit log at ~/.claude/fleet-bus-log.jsonl, mode 0o600 | Write JSONL, one event per line, chmod 600 |
| §8 | Every audit entry: ts, dir, subject | `subject` threaded through all callback signatures — inbound + outbound + drop |
| §8 | On in/out: envelope_id + req_id | Both required. Outbound path takes `req_id` param (Codex #5) |
| §8 | On drop: reason | Every drop path emits a §5-listed or implementation-specific `codex_adapter_<code>` reason |
| §9 | Heartbeat every 30s | 30-second interval task started in FleetBus.connect; envelope kind `status_heartbeat` |
| §9 | Payload minimum | `{online, process_alive_ts, pid, plugin_version}` |
| §10 | Compat suite in consumer's CI | `test/compat/` in codex-container, run in codex-container CI against a pinned fleet-bus tag |

## 8 — Vendoring (upstream-pinned)

Unchanged from v2. `vendored/envelope.v1.schema.json` + `vendored/FLEET_BUS_VERSION` (contains e.g. `v0.1.0`), Dockerfile `curl + diff` against raw upstream at the pinned tag.

## 9 — IDENTITY.md addition

Unchanged from v2. Model does not manually repeat `env_id`; `in_reply_to` set automatically.

## 10 — Env vars, deps, Docker

Unchanged from v2 apart from `FLEET_BUS_ENABLED` now explicitly documented as gate.

## 11 — Automatic `in_reply_to` semantics

When a bus envelope triggers a codex turn, `_process_bus_tags` receives `in_reply_to = incoming_envelope.id`. Any outbound `<BUS>` tag from that turn gets `in_reply_to` set automatically unless the model overrides via an explicit `in_reply_to="..."` attribute.

## 12 — Rollout order

1. **Skeleton**: `FleetBus` Python class in `bot.py` (or `fleet_bus.py`) with connect + subscribe + heartbeat + audit only. Feature flag off. Prove no regression when disabled.
2. **Lifecycle via setup_hook()**: prove bus starts on `FLEET_BUS_ENABLED=1`, cleanly disconnects on SIGTERM, does not duplicate on reconnect.
3. **Inbound path**: `_on_bus_envelope` + `_build_injection_frame` + `_ask_bus`. Prove: a probe from Kat to Vec lands in Vec's transcript as escaped `<channel source="fleet-bus">` frame.
4. **Outbound path**: `_find_bus_tags` + `_parse_attrs` + `_process_bus_tags` with quote-aware parser. Prove: Vec can `<BUS>`-reply to Kat, envelope round-trips.
5. **IDENTITY.md**: prompt-engineer `<BUS>` tag usage; iterate until reliable.
6. **Vendor + hash-pin** the schema; add Dockerfile check.
7. **Compat suite** in codex-container CI with the escaping + baton pass-through + reply-routing + quote-aware-parsing test cases named in §5, §6, §7.
8. Roll out to Ohm, then Myc, then Helm.

## 13 — Known risks

- **Model reliability on `<BUS>` tags.** Same tuning cost as `<CRON>` had.
- **Envelope size in `_ask_bus()`.** Truncate at 8KB in the frame if this becomes an issue.
- **Container restart drops in-flight state.** Acceptable for v1.
- **Bus-only turn output discarded.** Intentional — the model needs IDENTITY.md training to always wrap bus responses in a tag.
- **`<CRON>` in a bus turn is silently discarded.** Also intentional (Ohm v2 #4). If the model tries to schedule a job via a spoofed bus envelope, nothing happens.

## 14 — Not in scope for this design

- Subject-encoded sender (SPEC.md §4 planned closure) — separate PR against fleet-bus + nats.conf
- Baton protocol lifecycle (originator-side timeout, hop enforcement) — this adapter passes through fields, doesn't own semantics
- MCP notification path for codex — codex doesn't have one; we're deliberately using text-in-prompt via `_ask_bus`
