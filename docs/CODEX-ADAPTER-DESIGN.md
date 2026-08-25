# Codex-container fleet-bus adapter — design (v5)

**Target repo:** [`artifice-ia/codex-container`](https://github.com/artifice-ia/codex-container)
**Implements:** [SPEC.md](../SPEC.md) for the Python side
**Consumers who will run this:** Vec, Ohm, Myc, Helm
**Status:** design draft, not yet implemented

**Changes since v4** (in response to Ohm v4 + Codex re-review):

- **No mutable-global dereference across `await`.** `_on_bus_envelope` receives an explicit `bus_instance: FleetBus` parameter (bound at delivery time by the owning FleetBus). All audit/publish/validate calls go through the bound instance, not `global _bus`. If NATS reconnects mid-turn, the old handler still audits via the old (now-drained) instance's log file; publish attempts get `codex_adapter_publish_bus_stale`. New instance starts clean.
- **Every outbound audit has a non-null `req_id`.** Discord-triggered `<BUS>` tags generate a local nonce (`secrets.token_hex(16)`) via a helper. Bus-triggered turns pass the inbound `req_id` unchanged. Parser pre-routing failures use a non-null subject placeholder (`"parser"`) rather than `None` so §8's "subject on every entry" is actually satisfied.
- **Single-pass XML entity decoder.** Replaces sequential `.replace()` calls. `&amp;lt;` now correctly decodes to `&lt;`, not `<`. Cap the entity set to the five we emit (`&amp; &lt; &gt; &quot; &apos;`), reject unknown entities as literals rather than passing them through.
- **Malformed tags still stripped from text.** `_find_bus_tags` records the tag span even when parse fails, so `_process_bus_tags` removes the raw tag from the emitted-back text. A rejected `<BUS to = "..."/>` won't leak into a Discord reply.
- **Subject-safe kind validation for broadcast.** Kind used in a broadcast subject must match `[a-z0-9._-]+` per NATS subject rules (no whitespace, no `*`, no `>`). Reject as `codex_adapter_broadcast_kind_invalid` if it doesn't; a project-specific kind with spaces stays valid on direct subjects (`fleet.<to>.request`) where the kind doesn't hit the subject.

**Changes since v3:**

- **Backslash preservation in attribute parsing.** `\"` unescapes only when the outer attribute quote is `"` (and same for `'`). Any other `\<c>` passes through unchanged so JSON escapes (`\n`, `\t`, `\"` inside `'...'`-quoted attributes) survive to `json.loads`.
- **Invalid `hops` skips the whole outbound tag.** The v3 `continue` only advanced the inner `BATON_FIELDS` loop; malformed envelope was still publishing without hops. Now flagged with a per-tag sentinel that continues the outer loop.
- **Baton fields exposed in injection frame.** `_build_injection_frame` now emits `root_id`/`origin`/`owner`/`hops` as optional attributes when present on the inbound envelope. Field pass-through works both directions.
- **Parser-generated drop audits carry `subject`/`req_id`.** `_find_bus_tags` and `_parse_attrs` take a `context` param threaded through from the caller.
- **All adapter-specific audit reasons prefixed `codex_adapter_`.** SPEC §5 codes (unprefixed) reserved for the standardized reject taxonomy. Adapter-invented reasons (model_timeout, publish_error, envelope_frame_too_large, outbound_tag_*, prose_from_bus_turn_discarded, etc.) become `codex_adapter_<code>` per SPEC §8.

**Changes since v2:**

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
async def _on_bus_envelope(
    bus_instance: "FleetBus",
    subject: str,
    envelope: dict,
    req_id: str,
) -> None:
    """Invoked by FleetBus for each validated envelope. `bus_instance` is
    the owning FleetBus captured at delivery time — do NOT dereference
    the module-level `_bus` global across an await, because a reconnect
    can rebind it under our feet (Ohm v4 #1).

    `subject` is threaded through so every audit event includes it per
    SPEC §8."""

    # Alias for readability. bus is the bound instance; global _bus is
    # ignored below.
    bus = bus_instance

    try:
        frame = _build_injection_frame(envelope, req_id)
    except ValueError as e:
        bus.audit({
            "dir": "drop", "subject": subject,
            "envelope_id": envelope.get("id"), "req_id": req_id,
            "reason": (
                "codex_adapter_envelope_frame_too_large"
                if "1024" in str(e)
                else "codex_adapter_envelope_frame_invalid"
            ),
            "error": str(e),
        })
        return

    async with _lock:
        try:
            raw_reply = await _ask_bus(frame)
        except asyncio.TimeoutError:
            bus.audit({"dir": "drop", "subject": subject,
                       "envelope_id": envelope["id"], "req_id": req_id,
                       "reason": "codex_adapter_model_timeout"})
            return
        except Exception as e:
            bus.audit({"dir": "drop", "subject": subject,
                       "envelope_id": envelope["id"], "req_id": req_id,
                       "reason": "codex_adapter_model_error", "error": str(e)})
            return

    # BUS-ONLY: only _process_bus_tags runs. NOT _process_cron_tags.
    # Bus input must not trigger scheduler changes (Ohm v2 finding #4).
    cleaned, bus_notes = await _process_bus_tags(
        raw_reply,
        bus_instance=bus,             # v5 Ohm #1 — pass bound instance
        req_id=req_id,                # bus-triggered: reuse inbound nonce
        in_reply_to=envelope["id"],   # per §11, auto-set
        subject_context=subject,      # for audit events
    )

    # Non-tag prose from a bus turn is discarded. Ohm v1 finding #2 still holds.
    if cleaned.strip():
        bus.audit({"dir": "drop", "subject": subject,
                   "envelope_id": envelope["id"], "req_id": req_id,
                   "reason": "codex_adapter_prose_from_bus_turn_discarded",
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
    # v4 Ohm #2: baton fields must be preserved on inbound and visible to
    # the model in the injection frame. They're identifier-shaped
    # (correlate with audit log), so use _escape_identifier.
    extra = []
    for baton_field in ("root_id", "origin", "owner", "hops"):
        if baton_field in envelope and envelope[baton_field] is not None:
            extra.append(
                f'{baton_field}="{_escape_identifier(str(envelope[baton_field]))}"'
            )
    extra_str = (" " + " ".join(extra)) if extra else ""
    return (
        f'<channel source="fleet-bus" authenticated="false" '
        f'from_claim="{_escape_content(envelope["from"])}" '
        f'kind="{_escape_content(envelope["kind"])}" '
        f'env_id="{_escape_identifier(envelope["id"])}" '
        f'req_id="{_escape_identifier(req_id)}" '
        f'ts="{_escape_identifier(envelope["ts"])}"{extra_str}>\n'
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
def _find_bus_tags(
    text: str,
    bus: "FleetBus",
    context: dict | None = None,
) -> list[tuple[int, int, dict[str, str] | None]]:
    """Scan text for <BUS ... /> tags, respecting quoted attributes.
    Returns list of (start, end, attrs_or_None) tuples.

    `context` (subject/req_id from the caller) is included in drop audits
    per v4 Ohm #3.

    v5 Codex #2: malformed tags STILL get recorded in results with
    attrs=None so _process_bus_tags removes their spans from the emitted
    text. This prevents a rejected `<BUS to = 'x'/>` from leaking into a
    Discord reply."""
    context = context or {}
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

        # Walk forward, tracking quote state, until we hit '/>' at depth 0.
        # v4 Codex #1: only unescape backslash before the outer quote char.
        # Every other backslash passes through so JSON escapes in payload
        # attributes survive to json.loads (e.g., \n, \t, \" inside '...' quotes).
        pos = start + 4
        in_quote: str | None = None
        while pos < len(text):
            c = text[pos]
            if in_quote:
                if c == "\\" and pos + 1 < len(text) and text[pos + 1] == in_quote:
                    # \ specifically escaping the outer quote — consume both
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
                        # v5 Codex #2: still record the span for removal
                        # from text, even though we won't publish.
                        results.append((start, pos + 2, None))
                        bus.audit({
                            "dir": "drop",
                            "reason": "codex_adapter_outbound_tag_malformed",
                            "error": str(e),
                            "raw": text[start:pos + 2][:200],
                            **context,
                        })
                    i = pos + 2
                    break
                if c == "<":
                    # Nested tag detected — record the malformed span
                    # from <BUS to the unexpected <, so it gets stripped.
                    results.append((start, pos, None))
                    bus.audit({
                        "dir": "drop",
                        "reason": "codex_adapter_outbound_tag_unclosed",
                        "raw": text[start:pos][:200],
                        **context,
                    })
                    i = pos
                    break
                pos += 1
        else:
            # Ran off end of text — record from <BUS to end for stripping.
            results.append((start, len(text), None))
            bus.audit({
                "dir": "drop",
                "reason": "codex_adapter_outbound_tag_unclosed",
                "raw": text[start:start + 200],
                **context,
            })
            return results

def _parse_attrs(inner: str) -> dict[str, str]:
    """Parse `key="value" key='value'` pairs from tag inner. Handles both
    quote styles.

    v4 Codex #1: only unescape \\<outer_quote>. Every other backslash is
    preserved so JSON escapes inside `payload='...'` survive to
    json.loads. So `payload='{"text":"a\\nb"}'` yields the JSON string
    `{"text":"a\\nb"}` unchanged, and `json.loads` correctly interprets
    the `\\n` as a newline."""
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
        val_chars = []
        while pos < n and inner[pos] != quote:
            if inner[pos] == "\\" and pos + 1 < n and inner[pos + 1] == quote:
                # Backslash escaping the outer quote — consume the backslash,
                # emit the quote char. Any OTHER backslash (\n, \t, \\, etc)
                # passes through unchanged so json.loads sees it verbatim.
                val_chars.append(inner[pos + 1])
                pos += 2
            else:
                val_chars.append(inner[pos])
                pos += 1
        if pos >= n:
            raise ValueError(f"unclosed quote for {key!r}")
        pos += 1  # skip closing quote
        raw_value = "".join(val_chars)
        # v5 Codex #1 / Ohm #3: single-pass XML entity decode. Sequential
        # .replace() double-decodes: `&amp;lt;` → `&lt;` → `<` is wrong;
        # single-pass leaves the second entity alone since it wasn't in
        # the ORIGINAL text.
        attrs[key] = _decode_xml_entities(raw_value)
    return attrs

_ENTITY_MAP = {"amp": "&", "lt": "<", "gt": ">", "quot": '"', "apos": "'"}
_ENTITY_RE = re.compile(r"&([a-z]+);")

def _decode_xml_entities(text: str) -> str:
    """Single-pass decode of the five supported XML entities. Unknown
    entities pass through unchanged (as literal `&foo;`) rather than
    being reinterpreted.

    Correct behavior:
      &amp;         → &
      &lt;          → <
      &gt;          → >
      &quot;        → "
      &apos;        → '
      &amp;lt;      → &lt;      (NOT <)
      &amp;amp;    → &amp;      (single decoding)
      &unknown;    → &unknown;  (pass through)
    """
    def replace(m):
        return _ENTITY_MAP.get(m.group(1), m.group(0))
    return _ENTITY_RE.sub(replace, text)
```

Then in the processor:

```python
BATON_FIELDS = {"root_id", "origin", "owner", "hops"}

_NATS_SUBJECT_TOKEN_RE = re.compile(r"^[a-z0-9._-]+$")

def _pick_publish_subject(env: dict) -> str:
    """SPEC §6: replies go to fleet.<to>.result, requests go to fleet.<to>.request.
    Broadcasts (no `to`) go to fleet.broadcast.<kind>. Fix for Ohm v2 #1.

    v5 Codex #3: SPEC.md §3 allows arbitrary project-specific kinds, but
    for broadcasts the kind becomes a NATS subject token. Reject if the
    kind contains whitespace, `*`, `>`, or anything else that would break
    the subject. Direct routes don't have this problem since the kind
    doesn't hit the subject."""
    if env.get("to") is None:
        kind = env["kind"]
        if not _NATS_SUBJECT_TOKEN_RE.match(kind):
            raise ValueError(
                f"broadcast kind {kind!r} is not NATS-subject-safe "
                f"(must match [a-z0-9._-]+)"
            )
        return f"fleet.broadcast.{kind}"
    if env.get("in_reply_to"):
        return f"fleet.{env['to']}.result"
    return f"fleet.{env['to']}.request"

async def _process_bus_tags(
    text: str,
    bus_instance: "FleetBus",
    req_id: str | None = None,
    in_reply_to: str | None = None,
    subject_context: str | None = None,
) -> tuple[str, list[str]]:
    """Extract <BUS ... /> tags, publish each async with per-tag failure isolation.
    Returns (text-with-tags-removed, per-tag human-readable notes).

    v5 Ohm #1: bus_instance is the bound FleetBus (from callback delivery),
    not the module global. All audit/publish/validate calls use it.

    v5 Ohm #2: If req_id is None (Discord-originated turn), generate a
    local nonce so every audit has a non-null req_id per SPEC §8.
    subject_context defaults to 'parser' — never None — so pre-routing
    parser failures don't emit null subjects."""
    bus = bus_instance
    if req_id is None:
        req_id = secrets.token_hex(16)
    if subject_context is None:
        subject_context = "parser"

    # v4 Ohm #3: parser-generated drops carry the same context outbound audits do
    context = {"subject": subject_context, "req_id": req_id}
    tags = _find_bus_tags(text, bus=bus, context=context)
    if not tags:
        return text, []

    notes = []

    for start, end, attrs in tags:
        # v5 Codex #2: attrs is None when parser rejected the tag; the
        # tag's span is still in `tags` so it gets stripped from text.
        # Skip publish for parser-rejected tags.
        if attrs is None:
            continue
        try:
            payload = json.loads(attrs.get("payload", "{}"))
        except json.JSONDecodeError as e:
            notes.append(f"[bus tag ignored: payload JSON invalid: {e}]")
            bus.audit({
                "dir": "drop", "subject": subject_context,
                "reason": "codex_adapter_outbound_tag_payload_json_invalid",
                "raw": attrs.get("payload", "")[:200], "req_id": req_id,
            })
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

        # Baton additive-field pass-through. The model can set these on
        # the outbound tag; we preserve them without imposing semantics.
        #
        # v4 Codex #2 / Ohm #1: invalid hops MUST skip the whole tag,
        # not just the inner baton loop. Use a per-tag sentinel; continue
        # the outer tag loop on any baton parse failure.
        baton_invalid = False
        for baton_field in BATON_FIELDS:
            if baton_field in attrs:
                if baton_field == "hops":
                    try:
                        env["hops"] = int(attrs["hops"])
                    except ValueError:
                        notes.append(f"[bus tag ignored: hops not integer]")
                        bus.audit({
                            "dir": "drop", "subject": subject_context,
                            "reason": "codex_adapter_outbound_baton_hops_not_integer",
                            "req_id": req_id,
                        })
                        baton_invalid = True
                        break
                else:
                    env[baton_field] = attrs[baton_field]
        if baton_invalid:
            continue  # skip the whole tag

        validation = bus.validate_outbound(env)  # applies SPEC §5 reject codes
        if not validation["ok"]:
            notes.append(f"[bus tag ignored: {validation['error']}]")
            # SPEC §5 codes are unprefixed — this is the standardized taxonomy
            bus.audit({
                "dir": "drop", "subject": subject_context,
                "reason": validation["error"], "envelope_id": env["id"],
                "req_id": req_id,
            })
            continue

        try:
            subject = _pick_publish_subject(env)
        except ValueError as e:
            # v5 Codex #3: broadcast kind not subject-safe.
            notes.append(f"[bus tag ignored: {e}]")
            bus.audit({
                "dir": "drop", "subject": subject_context,
                "reason": "codex_adapter_broadcast_kind_invalid",
                "envelope_id": env["id"], "req_id": req_id,
                "error": str(e),
            })
            continue

        try:
            await bus.publish(subject, env)
            # SPEC §8: outbound audit MUST include envelope_id + req_id
            bus.audit({
                "dir": "out", "subject": subject,
                "envelope_id": env["id"], "req_id": req_id,
            })
            notes.append(
                f"→ bus: {env['from']}→{env.get('to') or 'broadcast'} "
                f"{env['kind']} ({env['id'][:8]}) via {subject}"
            )
        except Exception as e:
            notes.append(f"[bus publish failed: {e}]")
            # v5 Ohm #1: if bus was disconnected mid-turn, publish will
            # raise; classify as bus_stale rather than a generic error
            # for postmortem clarity.
            reason = (
                "codex_adapter_publish_bus_stale"
                if bus.is_closed()
                else "codex_adapter_publish_error"
            )
            bus.audit({
                "dir": "drop", "subject": subject,
                "reason": reason,
                "envelope_id": env["id"], "req_id": req_id,
                "error": str(e),
            })
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
