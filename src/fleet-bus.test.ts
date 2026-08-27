import { describe, expect, test } from 'bun:test'
import { connect, JSONCodec, type Msg, type NatsConnection } from 'nats'
import { mkdtempSync, readFileSync, writeFileSync } from 'node:fs'
import { homedir, tmpdir } from 'node:os'
import { join } from 'node:path'
import {
  BROADCAST_KIND_RE,
  BatonDerivationError,
  BatonHopsExhausted,
  DEFAULT_MAX_ENVELOPE_BYTES,
  DEFAULT_PAYLOAD_BODY_MAX_BYTES,
  FixedWindowBucket,
  FleetBus,
  RESERVED_BOT_NAMES,
  buildFleetBusFrame,
  buildFleetBusFrameMeta,
  buildFleetBusFramePayloadBody,
  createHeartbeatEnvelope,
  deriveBatonFields,
  loadFleetManifestAllowlist,
  normalizeAllowlist,
  normalizeBotName,
  payloadIsJsonSerializable,
  runSupervisor,
  validateEnvelope,
  type Envelope,
  type FleetBusRequestResult,
  type FleetBusSessionEvent,
  type TokenBucket,
} from './fleet-bus'

const jc = JSONCodec()
const allowlist = normalizeAllowlist(['luna', 'deet', 'kat', 'vec', 'ohm', 'myc', 'helm'])

function envelope(overrides: Record<string, unknown> = {}) {
  return {
    envelope_version: 1,
    id: 'bd132d42-3a78-4af4-86ad-fbcfe3dd811f',
    from: 'ohm',
    to: 'vec',
    kind: 'pr_review_request',
    ts: '2026-08-24T00:00:00.000Z',
    payload: { pr: 42 },
    ...overrides,
  }
}

describe('bot identity normalization', () => {
  test('canonicalizes case and NFKC-compatible glyphs', () => {
    expect(normalizeBotName('ＫＡＴ')).toBe('kat')
    expect(normalizeBotName('OhM')).toBe('ohm')
  })

  test('rejects homoglyphs, invisible characters, whitespace, and punctuation', () => {
    expect(normalizeBotName('kаt')).toBeNull() // Cyrillic small a
    expect(normalizeBotName('k​at')).toBeNull()
    expect(normalizeBotName(' kat')).toBeNull()
    expect(normalizeBotName('kat.bot')).toBeNull()
  })

  test('rejects reserved bot names', () => {
    expect(RESERVED_BOT_NAMES.has('broadcast')).toBe(true)
    expect(normalizeBotName('broadcast')).toBeNull()
    expect(normalizeBotName('BROADCAST')).toBeNull()
    expect(normalizeBotName('ＢＲＯＡＤＣＡＳＴ')).toBeNull()
    // Non-reserved lookalikes still normalize.
    expect(normalizeBotName('broadcasts')).toBe('broadcasts')
  })

  test('normalizes, deduplicates, and rejects invalid manifest entries', () => {
    expect([...normalizeAllowlist(['VEC', 'vec', 'myc'])]).toEqual(['vec', 'myc'])
    expect(() => normalizeAllowlist(['valid', 'not valid'])).toThrow(TypeError)
    expect(() => normalizeAllowlist(['broadcast'])).toThrow(TypeError)
  })
})

describe('kind vs bot-name regex split', () => {
  test('broadcast kind regex accepts dotted kinds', () => {
    expect(BROADCAST_KIND_RE.test('baton.start')).toBe(true)
    expect(BROADCAST_KIND_RE.test('baton.handoff')).toBe(true)
    expect(BROADCAST_KIND_RE.test('text_message')).toBe(true)
    expect(BROADCAST_KIND_RE.test('pr_review_result')).toBe(true)
  })

  test('broadcast kind regex rejects unsafe characters', () => {
    expect(BROADCAST_KIND_RE.test('baton .start')).toBe(false)
    expect(BROADCAST_KIND_RE.test('baton.start.')).toBe(false)
    expect(BROADCAST_KIND_RE.test('.baton')).toBe(false)
    expect(BROADCAST_KIND_RE.test('baton/start')).toBe(false)
    expect(BROADCAST_KIND_RE.test('')).toBe(false)
  })
})

describe('fleet manifest allowlist', () => {
  test('loads and canonicalizes bot_names from YAML', () => {
    const path = join(mkdtempSync(join(tmpdir(), 'fleet-manifest-')), 'fleet-manifest.yaml')
    writeFileSync(path, 'version: 1\nbot_names:\n  - Luna\n  - ＯＨＭ\n')
    expect([...loadFleetManifestAllowlist(path)]).toEqual(['luna', 'ohm'])
  })

  test('fails closed when bot_names is absent', () => {
    const path = join(mkdtempSync(join(tmpdir(), 'fleet-manifest-')), 'fleet-manifest.yaml')
    writeFileSync(path, 'version: 1\nhumans: [fernando]\n')
    expect(() => loadFleetManifestAllowlist(path)).toThrow('bot_names')
  })
})

describe('envelope validation', () => {
  test('accepts a v1 envelope and exposes only the normalized from claim', () => {
    const result = validateEnvelope(envelope({ from: 'ＯＨＭ' }), allowlist)
    expect(result.ok).toBe(true)
    if (result.ok) expect(result.envelope.from).toBe('ohm')
  })

  test.each([
    ['non-object', null, 'envelope_not_object'],
    ['wrong version', envelope({ envelope_version: 2 }), 'unsupported_envelope_version'],
    ['missing id', envelope({ id: '' }), 'invalid_id'],
    ['missing kind', envelope({ kind: '' }), 'invalid_kind'],
    ['bad timestamp', envelope({ ts: 'yesterday-ish' }), 'invalid_ts'],
    ['missing payload', (() => { const value = envelope(); delete (value as Record<string, unknown>).payload; return value })(), 'missing_payload'],
    ['unknown sender', envelope({ from: 'fernando' }), 'from_claim_rejected'],
    ['homoglyph sender', envelope({ from: 'оhm' }), 'from_claim_rejected'],
  ])('rejects %s', (_label, value, error) => {
    expect(validateEnvelope(value, allowlist)).toEqual({ ok: false, error })
  })

  test('rejects envelopes above the encoded byte limit', () => {
    const value = envelope({ payload: 'x'.repeat(DEFAULT_MAX_ENVELOPE_BYTES) })
    expect(validateEnvelope(value, allowlist)).toEqual({ ok: false, error: 'envelope_too_large' })
  })

  describe('baton extension', () => {
    test('accepts a well-formed baton envelope', () => {
      const result = validateEnvelope(envelope({
        root_id: 'root-uuid-1', origin: 'ohm', owner: 'vec', hops: 2,
      }), allowlist)
      expect(result.ok).toBe(true)
    })

    test.each([
      ['root_id empty string', { root_id: '' }, 'claude_discord_adapter_invalid_root_id'],
      ['root_id non-string', { root_id: 42 }, 'claude_discord_adapter_invalid_root_id'],
      ['origin non-canonical case', { origin: 'OHM' }, 'claude_discord_adapter_invalid_origin'],
      ['origin not in allowlist', { origin: 'fernando' }, 'claude_discord_adapter_invalid_origin'],
      ['origin reserved', { origin: 'broadcast' }, 'claude_discord_adapter_invalid_origin'],
      ['owner homoglyph', { owner: 'оhm' }, 'claude_discord_adapter_invalid_owner'],
      ['hops negative', { hops: -1 }, 'claude_discord_adapter_invalid_hops'],
      ['hops NaN', { hops: Number.NaN }, 'claude_discord_adapter_invalid_hops'],
      ['hops float', { hops: 1.5 }, 'claude_discord_adapter_invalid_hops'],
      ['hops string', { hops: '3' }, 'claude_discord_adapter_invalid_hops'],
      ['hops at ceiling', { hops: 16 }, 'claude_discord_adapter_invalid_hops_ceiling'],
      ['hops above ceiling', { hops: 100 }, 'claude_discord_adapter_invalid_hops_ceiling'],
    ])('rejects %s', (_label, patch, error) => {
      expect(validateEnvelope(envelope(patch), allowlist)).toEqual({ ok: false, error })
    })

    test('hops = 0 is valid', () => {
      const result = validateEnvelope(envelope({ hops: 0 }), allowlist)
      expect(result.ok).toBe(true)
    })

    test('hops = 15 is valid (just under ceiling)', () => {
      const result = validateEnvelope(envelope({ hops: 15 }), allowlist)
      expect(result.ok).toBe(true)
    })
  })
})

describe('deriveBatonFields', () => {
  test('originating request seeds the chain', () => {
    const fields = deriveBatonFields({ envelopeId: 'e-1', botName: 'vec', kind: 'text_message' })
    expect(fields).toEqual({ root_id: 'e-1', origin: 'vec', owner: 'vec', hops: 0 })
  })

  test('response propagates root_id/origin/owner and increments hops', () => {
    const inbound = envelope({ root_id: 'r-1', origin: 'ohm', owner: 'ohm', hops: 3 }) as Envelope
    const fields = deriveBatonFields({ envelopeId: 'e-2', botName: 'vec', kind: 'text_message', inbound })
    expect(fields).toEqual({ root_id: 'r-1', origin: 'ohm', owner: 'ohm', hops: 4 })
  })

  test('response falls back to inbound.id/from when baton fields absent', () => {
    const inbound = envelope({ id: 'wire-id-1' }) as Envelope
    const fields = deriveBatonFields({ envelopeId: 'e-3', botName: 'vec', kind: 'text_message', inbound })
    expect(fields).toEqual({ root_id: 'wire-id-1', origin: 'ohm', owner: 'ohm', hops: 1 })
  })

  test('baton.handoff overrides owner with normalized recipient', () => {
    const inbound = envelope({ root_id: 'r-1', origin: 'ohm', owner: 'ohm', hops: 1 }) as Envelope
    const fields = deriveBatonFields({
      envelopeId: 'e-4', botName: 'vec', kind: 'baton.handoff', recipient: 'ＫＡＴ', inbound,
    })
    expect(fields.owner).toBe('kat')
  })

  test('baton.handoff with invalid recipient throws', () => {
    expect(() => deriveBatonFields({
      envelopeId: 'e-5', botName: 'vec', kind: 'baton.handoff', recipient: 'not.a.bot',
    })).toThrow(BatonDerivationError)
  })

  test('baton.handoff with reserved recipient throws', () => {
    expect(() => deriveBatonFields({
      envelopeId: 'e-6', botName: 'vec', kind: 'baton.handoff', recipient: 'broadcast',
    })).toThrow(BatonDerivationError)
  })

  test('hops >= 16 throws BatonHopsExhausted', () => {
    const inbound = envelope({ hops: 15 }) as Envelope
    expect(() => deriveBatonFields({
      envelopeId: 'e-7', botName: 'vec', kind: 'text_message', inbound,
    })).toThrow(BatonHopsExhausted)
  })

  test('non-integer inbound hops treated as absent (defaults to 0 → new hops = 1)', () => {
    // Number.isInteger(NaN) is false — this defends against typeof-number NaN slipping through.
    const inbound = envelope({ hops: Number.NaN }) as unknown as Envelope
    const fields = deriveBatonFields({ envelopeId: 'e-8', botName: 'vec', kind: 'text_message', inbound })
    expect(fields.hops).toBe(1)
  })

  test('invalid bot name throws', () => {
    expect(() => deriveBatonFields({ envelopeId: 'e-9', botName: 'not.a.bot', kind: 'text_message' }))
      .toThrow(BatonDerivationError)
  })
})

describe('status heartbeat', () => {
  test('uses the v1 envelope and identifies its sender', () => {
    const heartbeat = createHeartbeatEnvelope('ＫＡＴ', '0.4.0', 1234, new Date('2026-08-24T18:36:25.889Z'))

    expect(heartbeat).toMatchObject({
      envelope_version: 1,
      from: 'kat',
      to: null,
      kind: 'status_heartbeat',
      ts: '2026-08-24T18:36:25.889Z',
      payload: {
        online: true,
        process_alive_ts: '2026-08-24T18:36:25.889Z',
        pid: 1234,
        plugin_version: '0.4.0',
      },
    })
    expect(heartbeat.id).toMatch(/^[a-f0-9-]{36}$/)
    expect(validateEnvelope(heartbeat, normalizeAllowlist(['kat'])).ok).toBe(true)
  })
})

/* -------------------------------------------------------------------------- */
/* Fake NATS connection — enough surface for FleetBus unit tests              */
/* -------------------------------------------------------------------------- */

interface FakeSubscription {
  subject: string
  push: (msg: Msg) => void
  unsubscribe: () => void
}

class FakeNatsConnection {
  publishes: Array<{ subject: string; envelope: unknown }> = []
  subscribed: string[] = []
  private closed_ = false
  private readonly closedResolve: () => void
  readonly closedPromise: Promise<void>
  private readonly subscriptions: FakeSubscription[] = []

  constructor() {
    let resolve: () => void = () => {}
    this.closedPromise = new Promise<void>(r => { resolve = r })
    this.closedResolve = resolve
  }

  publish(subject: string, data: Uint8Array): void {
    if (this.closed_) throw new Error('closed')
    this.publishes.push({ subject, envelope: jc.decode(data) })
  }

  subscribe(subject: string): { unsubscribe: () => void; [Symbol.asyncIterator]: () => AsyncIterator<Msg> } {
    this.subscribed.push(subject)
    const queue: Msg[] = []
    let notify: (() => void) | null = null
    let done = false
    const self = this
    const push = (msg: Msg): void => {
      queue.push(msg)
      const n = notify
      notify = null
      n?.()
    }
    const unsubscribe = (): void => {
      done = true
      const n = notify
      notify = null
      n?.()
    }
    this.subscriptions.push({ subject, push, unsubscribe })
    return {
      unsubscribe,
      [Symbol.asyncIterator](): AsyncIterator<Msg> {
        return {
          async next(): Promise<IteratorResult<Msg>> {
            while (queue.length === 0) {
              if (done || self.closed_) return { value: undefined as unknown as Msg, done: true }
              await new Promise<void>(r => { notify = r })
            }
            return { value: queue.shift()!, done: false }
          },
        }
      },
    }
  }

  isClosed(): boolean { return this.closed_ }
  async close(): Promise<void> { this.markClosed() }
  async drain(): Promise<void> { this.markClosed() }
  closed(): Promise<void> { return this.closedPromise }
  status(): AsyncIterable<unknown> {
    return { [Symbol.asyncIterator]: (): AsyncIterator<unknown> => ({ next: (): Promise<IteratorResult<unknown>> => new Promise(() => {}) }) }
  }

  markClosed(): void {
    if (this.closed_) return
    this.closed_ = true
    for (const sub of this.subscriptions) sub.unsubscribe()
    this.closedResolve()
  }
}

function fakeMessage(subject: string, envelope: unknown): Msg {
  return { subject, data: jc.encode(envelope) } as Msg
}

class TestFleetBus extends FleetBus {
  attachFakeNc(nc: FakeNatsConnection): void {
    (this as unknown as { nc: FakeNatsConnection }).nc = nc
  }
  handleRequest(value: unknown, subject = 'fleet.vec.request'): Promise<void> {
    return this.onRequest(fakeMessage(subject, value))
  }
  handleResult(value: unknown, subject = 'fleet.vec.result'): Promise<void> {
    return this.onResult(fakeMessage(subject, value))
  }
  outboundLedgerSize(): number {
    return (this as unknown as { outboundLedger: { size: number } }).outboundLedger.size
  }
  receiveLedgerSize(): number {
    return (this as unknown as { receiveLedger: { size: number } }).receiveLedger.size
  }
  evictedLedgerHas(id: string): boolean {
    return (this as unknown as { evictedLedger: { has: (k: string) => boolean } }).evictedLedger.has(id)
  }
}

describe('request session injection', () => {
  test('injects an allowlisted envelope with a distinct server nonce', async () => {
    const events: Array<{ envelope: { from: string }; reqId: string }> = []
    const bus = new TestFleetBus({
      botName: 'vec', url: 'nats://unused', user: 'vec', password: 'unused',
      injectIntoSession: async event => { events.push(event) },
    }, allowlist)

    const value = envelope({ from: 'ＯＨＭ' })
    await bus.handleRequest(value)

    expect(events).toHaveLength(1)
    expect(events[0]!.envelope.from).toBe('ohm')
    expect(events[0]!.reqId).toMatch(/^[a-f0-9]{32}$/)
    expect(events[0]!.reqId).not.toBe(value.id)
  })

  test('frame metadata exposes both the server nonce and wire envelope id', () => {
    const value = envelope()
    expect(buildFleetBusFrameMeta({ envelope: value as Envelope, reqId: 'a'.repeat(32) })).toEqual({
      source: 'fleet-bus',
      authenticated: 'false',
      from_claim: 'ohm',
      kind: 'pr_review_request',
      req_id: 'a'.repeat(32),
      env_id: value.id,
      ts: value.ts,
    })
  })

  test.each(['fernando', 'unknown', 'оhm'])('rejects from_claim %s and audits the drop', async from => {
    const dir = mkdtempSync(join(tmpdir(), 'fleet-audit-'))
    const auditLogPath = join(dir, 'fleet-bus.jsonl')
    let injected = false
    const bus = new TestFleetBus({
      botName: 'vec', url: 'nats://unused', user: 'vec', password: 'unused', auditLogPath,
      injectIntoSession: async () => { injected = true },
    }, allowlist)

    await bus.handleRequest(envelope({ from }))

    expect(injected).toBe(false)
    expect(readFileSync(auditLogPath, 'utf8')).toContain('from_claim_rejected')
  })

  test.each([
    ['another bot', 'kat'],
    ['null', null],
    ['a missing recipient', undefined],
  ])('rejects direct requests addressed to %s', async (_label, to) => {
    const dir = mkdtempSync(join(tmpdir(), 'fleet-audit-'))
    const auditLogPath = join(dir, 'fleet-bus.jsonl')
    let injected = false
    const bus = new TestFleetBus({
      botName: 'vec', url: 'nats://unused', user: 'vec', password: 'unused', auditLogPath,
      injectIntoSession: async () => { injected = true },
    }, allowlist)

    await bus.handleRequest(envelope({ to }))

    expect(injected).toBe(false)
    expect(readFileSync(auditLogPath, 'utf8')).toContain('recipient_mismatch')
  })

  test('receive ledger is bounded — oldest reqId evicts on overflow', async () => {
    const events: FleetBusSessionEvent[] = []
    const bus = new TestFleetBus({
      botName: 'vec', url: 'nats://unused', user: 'vec', password: 'unused',
      receiveLedgerCap: 2,
      injectIntoSession: async event => { events.push(event) },
    }, allowlist)
    bus.attachFakeNc(new FakeNatsConnection())
    await bus.handleRequest(envelope({ from: 'ohm', id: 'wire-1' }))
    await bus.handleRequest(envelope({ from: 'ohm', id: 'wire-2' }))
    await bus.handleRequest(envelope({ from: 'ohm', id: 'wire-3' }))
    expect(bus.receiveLedgerSize()).toBe(2)
    // First reqId should now be evicted — publishReply on it returns req_id_unknown.
    const first = events[0]!.reqId
    const result = bus.publishReply(first, {})
    expect(result.error).toBe('req_id_unknown')
  })

  test('populates the receive ledger keyed by the local reqId', async () => {
    const events: FleetBusSessionEvent[] = []
    const bus = new TestFleetBus({
      botName: 'vec', url: 'nats://unused', user: 'vec', password: 'unused',
      injectIntoSession: async event => { events.push(event) },
    }, allowlist)
    await bus.handleRequest(envelope({ from: 'ohm', id: 'wire-99' }))
    expect(bus.receiveLedgerSize()).toBe(1)
    // publishReply on the received reqId should now find the inbound envelope
    // (although publish will fail with fleet_bus_not_connected — that's OK, it
    // proves the ledger lookup succeeded).
    const reply = bus.publishReply(events[0]!.reqId, { ok: true })
    expect(reply.error).toBe('fleet_bus_not_connected')
  })
})

/* -------------------------------------------------------------------------- */
/* Frame escape + payload body cap                                             */
/* -------------------------------------------------------------------------- */

describe('frame escaping and caps', () => {
  test('escapes XML entities in content fields', () => {
    const meta = buildFleetBusFrameMeta({
      envelope: envelope({ kind: 'text<message>&test' }) as Envelope,
      reqId: 'r-1',
    })
    expect(meta.kind).toBe('text&lt;message&gt;&amp;test')
  })

  test('escapes attribute-value special characters', () => {
    const meta = buildFleetBusFrameMeta({
      envelope: envelope({ kind: 'a"b\'c' }) as Envelope,
      reqId: 'r-1',
    })
    expect(meta.kind).toBe('a&quot;b&apos;c')
  })

  test('strips zero-width characters from human-visible fields', () => {
    const meta = buildFleetBusFrameMeta({
      envelope: envelope({ from: 'ohm', kind: 'te​xt‎msg' }) as Envelope,
      reqId: 'r-1',
    })
    expect(meta.kind).toBe('textmsg')
  })

  test('does NOT strip zero-width from env_id or req_id (audit correlation)', () => {
    const dirtyId = 'abc​123'
    const meta = buildFleetBusFrameMeta({
      envelope: envelope({ id: dirtyId }) as Envelope,
      reqId: 'req​xyz',
    })
    expect(meta.env_id).toBe(dirtyId)
    expect(meta.req_id).toBe('req​xyz')
  })

  test('payload body cap: short payload passes through with XML escape', () => {
    const env = envelope({ payload: { hello: 'world' } }) as Envelope
    const { body, truncated } = buildFleetBusFramePayloadBody(env)
    expect(truncated).toBe(false)
    // JSON quotes are XML-escaped so the body embeds safely inside <payload>...</payload>.
    expect(body).toBe('{&quot;hello&quot;:&quot;world&quot;}')
  })

  test('payload body cap: oversized payload gets truncated with marker', () => {
    const env = envelope({ id: 'huge-1', payload: 'x'.repeat(DEFAULT_PAYLOAD_BODY_MAX_BYTES * 2) }) as Envelope
    const { body, truncated } = buildFleetBusFramePayloadBody(env)
    expect(truncated).toBe(true)
    expect(body).toContain('[...truncated 8KB max, full envelope in audit log env_id=huge-1]')
    // Body content up to marker should be ≤ cap (plus JSON quote chars).
    const markerIdx = body.indexOf('\n[...truncated')
    expect(markerIdx).toBeGreaterThan(0)
    expect(markerIdx).toBeLessThanOrEqual(DEFAULT_PAYLOAD_BODY_MAX_BYTES + 8)
  })

  test('payload body cap is measured POST-escape (5× expansion for `&` payloads)', () => {
    // 4KB of raw `&` chars → 20KB of escaped `&amp;` output.
    const rawAmpBytes = 4_096
    const env = envelope({ id: 'ampersand-1', payload: '&'.repeat(rawAmpBytes) }) as Envelope
    const { body, truncated } = buildFleetBusFramePayloadBody(env)
    expect(truncated).toBe(true)
    // The pre-marker body slice MUST NOT exceed the cap even with the escape expansion.
    const markerStart = body.indexOf('\n[...truncated')
    const preMarker = body.slice(0, markerStart)
    expect(Buffer.byteLength(preMarker, 'utf8')).toBeLessThanOrEqual(DEFAULT_PAYLOAD_BODY_MAX_BYTES)
    // Trailing chars must be a complete entity (`&amp;`), not a partial one like `&am`.
    expect(preMarker.endsWith('&amp;') || preMarker.endsWith('"')).toBe(true)
  })

  test('trims trailing partial XML entity at truncation boundary', () => {
    // Payload full of `&` — the truncation is deterministic enough to land mid-entity.
    // Just verify no half-entity leaks: no `&` without matching `;` between it and the marker.
    const env = envelope({ id: 'amp-2', payload: '&'.repeat(4_096) }) as Envelope
    const { body } = buildFleetBusFramePayloadBody(env)
    const preMarker = body.slice(0, body.indexOf('\n[...truncated'))
    const lastAmp = preMarker.lastIndexOf('&')
    if (lastAmp !== -1) {
      const tail = preMarker.slice(lastAmp)
      expect(tail.includes(';')).toBe(true)
    }
  })

  test('full frame builds with attribute escape and body cap', () => {
    const env = envelope({ payload: { note: 'hi <them>' } }) as Envelope
    const frame = buildFleetBusFrame({ envelope: env, reqId: 'r-2' })
    expect(frame).toContain('source="fleet-bus"')
    expect(frame).toContain('authenticated="false"')
    expect(frame).toContain('from_claim="ohm"')
    expect(frame).toContain(`env_id="${env.id}"`)
    expect(frame).toContain('req_id="r-2"')
    // Payload body is XML-escaped.
    expect(frame).toContain('hi &lt;them&gt;')
  })

  test('full frame includes baton attributes when present', () => {
    const env = envelope({ root_id: 'r-1', origin: 'ohm', owner: 'vec', hops: 3 }) as Envelope
    const frame = buildFleetBusFrame({ envelope: env, reqId: 'r-3' })
    expect(frame).toContain('root_id="r-1"')
    expect(frame).toContain('origin="ohm"')
    expect(frame).toContain('owner="vec"')
    expect(frame).toContain('hops="3"')
  })

  test('full frame includes late_reply_env_id when tagged', () => {
    const env = envelope() as Envelope
    const frame = buildFleetBusFrame({ envelope: env, reqId: 'r-4', unsolicited: true, lateReplyEnvId: 'old-wire-id-1' })
    expect(frame).toContain('late_reply_env_id="old-wire-id-1"')
  })
})

/* -------------------------------------------------------------------------- */
/* request() + publishReply() + onResult                                       */
/* -------------------------------------------------------------------------- */

function readAudit(path: string): Array<Record<string, unknown>> {
  const text = readFileSync(path, 'utf8').trim()
  if (!text) return []
  return text.split('\n').map(line => JSON.parse(line))
}

describe('request / publishReply / onResult', () => {
  test('request({wait:false}) publishes and returns immediately', async () => {
    const nc = new FakeNatsConnection()
    const bus = new TestFleetBus({
      botName: 'vec', url: 'nats://unused', user: 'vec', password: 'unused',
    }, allowlist)
    bus.attachFakeNc(nc)

    const result = await bus.request({ to: 'kat', kind: 'text_message', payload: { hi: true } })
    expect(result.ok).toBe(true)
    // Core NATS publish gives no proof of subscriber receipt — this field
    // is reserved for the wait:true + ledger-matched-reply path (P1 round 2).
    expect(result.delivered_to_subscriber).toBeUndefined()
    expect(result.reply).toBeUndefined()
    expect(nc.publishes).toHaveLength(1)
    expect(nc.publishes[0]!.subject).toBe('fleet.kat.request')
    const publishedEnv = nc.publishes[0]!.envelope as Envelope
    expect(publishedEnv.from).toBe('vec')
    expect(publishedEnv.to).toBe('kat')
    expect(publishedEnv.root_id).toBe(publishedEnv.id)
    expect(publishedEnv.origin).toBe('vec')
    expect(publishedEnv.owner).toBe('vec')
    expect(publishedEnv.hops).toBe(0)
  })

  test('request({wait:true}) awaits ledger-matched reply', async () => {
    const nc = new FakeNatsConnection()
    const injections: FleetBusSessionEvent[] = []
    const bus = new TestFleetBus({
      botName: 'vec', url: 'nats://unused', user: 'vec', password: 'unused',
      injectIntoSession: async event => { injections.push(event) },
    }, allowlist)
    bus.attachFakeNc(nc)

    const promise = bus.request({ to: 'kat', kind: 'text_message', payload: {}, wait: true, timeoutMs: 5_000 })
    // Give the promise a microtask to register in the ledger.
    await Promise.resolve()
    expect(bus.outboundLedgerSize()).toBe(1)
    const publishedEnv = nc.publishes[0]!.envelope as Envelope

    const reply = {
      envelope_version: 1, id: 'reply-uuid-1', from: 'kat', to: 'vec',
      kind: 'result', in_reply_to: publishedEnv.id,
      ts: '2026-08-27T00:00:00.000Z', payload: { done: true },
    }
    await bus.handleResult(reply)
    const result = await promise
    expect(result.ok).toBe(true)
    expect(result.reply?.id).toBe('reply-uuid-1')
    // Ledger-matched reply MUST NOT be injected into the session (suppression rule).
    expect(injections).toHaveLength(0)
    expect(bus.outboundLedgerSize()).toBe(0)
  })

  test('request({wait:true}) resolves timed_out after timeoutMs elapses', async () => {
    const nc = new FakeNatsConnection()
    const bus = new TestFleetBus({
      botName: 'vec', url: 'nats://unused', user: 'vec', password: 'unused',
    }, allowlist)
    bus.attachFakeNc(nc)
    const result = await bus.request({ to: 'kat', kind: 'text_message', payload: {}, wait: true, timeoutMs: 25 })
    expect(result.ok).toBe(false)
    expect(result.timed_out).toBe(true)
    expect(bus.outboundLedgerSize()).toBe(0)
    // Evicted ledger tracked for late-reply tagging.
    expect(bus.evictedLedgerHas(result.envelope!.id)).toBe(true)
  })

  test('LRU eviction of an outstanding waiter rejects with ledger_overflow', async () => {
    const nc = new FakeNatsConnection()
    const bus = new TestFleetBus({
      botName: 'vec', url: 'nats://unused', user: 'vec', password: 'unused',
      inflightLedgerCap: 2,
    }, allowlist)
    bus.attachFakeNc(nc)

    const p1 = bus.request({ to: 'kat', kind: 'text_message', payload: { i: 1 }, wait: true, timeoutMs: 5_000 })
    const p2 = bus.request({ to: 'kat', kind: 'text_message', payload: { i: 2 }, wait: true, timeoutMs: 5_000 })
    // p3 admission overflows the cap → p1 (oldest) evicts with ledger_overflow.
    const p3 = bus.request({ to: 'kat', kind: 'text_message', payload: { i: 3 }, wait: true, timeoutMs: 5_000 })
    // Consume p2/p3 so bun doesn't flag them as unhandled at teardown.
    void p2.then(() => {}, () => {})
    void p3.then(() => {}, () => {})

    const r1 = await p1
    expect(r1.ok).toBe(false)
    expect(r1.error).toBe('ledger_overflow')
    // Evicted id is tracked so a later reply arriving surfaces as a late-reply.
    expect(bus.evictedLedgerHas(r1.envelope!.id)).toBe(true)
    // p2 and p3 remain pending in the ledger; we don't await them here.
    expect(bus.outboundLedgerSize()).toBe(2)
  })

  test('publishReply returns req_id_unknown when reqId is absent from the receive ledger', () => {
    const nc = new FakeNatsConnection()
    const bus = new TestFleetBus({
      botName: 'vec', url: 'nats://unused', user: 'vec', password: 'unused',
    }, allowlist)
    bus.attachFakeNc(nc)
    const result = bus.publishReply('never-received-nonce', { ok: true })
    expect(result.ok).toBe(false)
    expect(result.error).toBe('req_id_unknown')
    expect(nc.publishes).toHaveLength(0)
  })

  test('publishReply publishes to inbound.from.result with in_reply_to = inbound.id (NOT the local reqId)', async () => {
    const nc = new FakeNatsConnection()
    const events: FleetBusSessionEvent[] = []
    const bus = new TestFleetBus({
      botName: 'vec', url: 'nats://unused', user: 'vec', password: 'unused',
      injectIntoSession: async event => { events.push(event) },
    }, allowlist)
    bus.attachFakeNc(nc)
    // Receive an inbound envelope so the receive ledger has an entry.
    await bus.handleRequest(envelope({ from: 'ohm', id: 'wire-42', to: 'vec' }))
    expect(events).toHaveLength(1)
    const reqId = events[0]!.reqId

    const result = bus.publishReply(reqId, { done: true }, 'pr_review_result')
    expect(result.ok).toBe(true)
    expect(nc.publishes.map(p => p.subject)).toEqual(['fleet.ohm.result'])
    const publishedReply = nc.publishes[0]!.envelope as Envelope
    // Wire in_reply_to MUST be the inbound wire id, not the consumer-local reqId (round-2 P1).
    expect(publishedReply.in_reply_to).toBe('wire-42')
    expect(publishedReply.in_reply_to).not.toBe(reqId)
    expect(publishedReply.to).toBe('ohm')
    expect(publishedReply.from).toBe('vec')
    // Baton propagated from inbound (hops=0 inbound → hops=1 reply).
    expect(publishedReply.root_id).toBe('wire-42')
    expect(publishedReply.hops).toBe(1)
  })

  test.each([
    ['top-level undefined', undefined],
    ['top-level function', () => { /* noop */ }],
    ['top-level symbol', Symbol('x')],
    ['top-level bigint', BigInt(1)],
    ['top-level NaN', Number.NaN],
    ['top-level Infinity', Number.POSITIVE_INFINITY],
    ['top-level -Infinity', Number.NEGATIVE_INFINITY],
    ['nested undefined', { ok: true, data: undefined }],
    ['deeply nested undefined', { a: { b: { c: undefined } } }],
    ['nested function', { ok: true, cb: () => 1 }],
    ['nested symbol', { ok: true, s: Symbol('nested') }],
    ['nested bigint', { ok: true, n: BigInt(42) }],
    ['nested NaN', { ok: true, score: Number.NaN }],
    ['nested Infinity', { ok: true, limit: Number.POSITIVE_INFINITY }],
    ['NaN inside array', [1, Number.NaN, 3]],
    ['undefined inside array', [1, undefined, 3]],
  ])('request() rejects payload that disappears in JSON encoding — %s', async (_label, payload) => {
    const nc = new FakeNatsConnection()
    const bus = new TestFleetBus({
      botName: 'vec', url: 'nats://unused', user: 'vec', password: 'unused',
    }, allowlist)
    bus.attachFakeNc(nc)
    const result = await bus.request({ to: 'kat', kind: 'text_message', payload })
    expect(result.ok).toBe(false)
    expect(result.error).toBe('claude_discord_adapter_payload_not_json_serializable')
    // Nothing hit the wire.
    expect(nc.publishes).toHaveLength(0)
  })

  test('payloadIsJsonSerializable accepts well-formed payloads', () => {
    expect(payloadIsJsonSerializable({})).toBe(true)
    expect(payloadIsJsonSerializable({ a: 1, b: 'x', c: null, d: [1, 2] })).toBe(true)
    expect(payloadIsJsonSerializable([])).toBe(true)
    expect(payloadIsJsonSerializable('string')).toBe(true)
    expect(payloadIsJsonSerializable(0)).toBe(true)
    expect(payloadIsJsonSerializable(null)).toBe(true)
    expect(payloadIsJsonSerializable(false)).toBe(true)
  })

  test('publishReply() rejects unencodable payload', async () => {
    const nc = new FakeNatsConnection()
    const events: FleetBusSessionEvent[] = []
    const bus = new TestFleetBus({
      botName: 'vec', url: 'nats://unused', user: 'vec', password: 'unused',
      injectIntoSession: async event => { events.push(event) },
    }, allowlist)
    bus.attachFakeNc(nc)
    await bus.handleRequest(envelope({ from: 'ohm', id: 'wire-99' }))
    const reqId = events[0]!.reqId
    // Also verify nested case here — round-2 P2 was specifically about the
    // nested-undefined silent-drop the top-level guard missed.
    const result = bus.publishReply(reqId, { ok: true, data: undefined })
    expect(result.ok).toBe(false)
    expect(result.error).toBe('claude_discord_adapter_payload_not_json_serializable')
    expect(nc.publishes).toHaveLength(0)
  })

  test('outbound request audit carries req_id alongside envelope_id (SPEC §8)', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'fleet-audit-'))
    const auditLogPath = join(dir, 'fleet-bus.jsonl')
    const nc = new FakeNatsConnection()
    const bus = new TestFleetBus({
      botName: 'vec', url: 'nats://unused', user: 'vec', password: 'unused', auditLogPath,
    }, allowlist)
    bus.attachFakeNc(nc)
    const result = await bus.request({ to: 'kat', kind: 'text_message', payload: { hi: true } })
    expect(result.ok).toBe(true)
    const entries = readAudit(auditLogPath)
    const outEntry = entries.find(e => e.dir === 'out')
    expect(outEntry).toBeDefined()
    expect(outEntry!.envelope_id).toBe(result.envelope!.id)
    expect(typeof outEntry!.req_id).toBe('string')
    expect((outEntry!.req_id as string)).toMatch(/^[a-f0-9]{32}$/)
  })

  test('wait:true + ledger-matched reply still sets delivered_to_subscriber:true', async () => {
    const nc = new FakeNatsConnection()
    const bus = new TestFleetBus({
      botName: 'vec', url: 'nats://unused', user: 'vec', password: 'unused',
    }, allowlist)
    bus.attachFakeNc(nc)
    const promise = bus.request({ to: 'kat', kind: 'text_message', payload: {}, wait: true, timeoutMs: 5_000 })
    await Promise.resolve()
    const publishedEnv = nc.publishes[0]!.envelope as Envelope
    const reply = {
      envelope_version: 1, id: 'reply-uuid-2', from: 'kat', to: 'vec',
      kind: 'result', in_reply_to: publishedEnv.id,
      ts: '2026-08-27T00:00:00.000Z', payload: { done: true },
    }
    await bus.handleResult(reply)
    const result = await promise
    // Only the wait:true + reply-received path may claim delivery.
    expect(result.delivered_to_subscriber).toBe(true)
    expect(result.reply?.id).toBe('reply-uuid-2')
  })

  test('matched-reply audit preserves the ORIGINAL request req_id (log correlation)', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'fleet-audit-'))
    const auditLogPath = join(dir, 'fleet-bus.jsonl')
    const nc = new FakeNatsConnection()
    const bus = new TestFleetBus({
      botName: 'vec', url: 'nats://unused', user: 'vec', password: 'unused', auditLogPath,
    }, allowlist)
    bus.attachFakeNc(nc)
    const promise = bus.request({ to: 'kat', kind: 'text_message', payload: {}, wait: true, timeoutMs: 5_000 })
    await Promise.resolve()
    const publishedEnv = nc.publishes[0]!.envelope as Envelope
    // Grab the request's outbound audit — that's the req_id we expect the
    // matching reply's inbound audit to carry.
    const outEntry = readAudit(auditLogPath).find(e => e.dir === 'out')
    expect(outEntry).toBeDefined()
    const requestReqId = outEntry!.req_id as string
    expect(typeof requestReqId).toBe('string')

    const reply = {
      envelope_version: 1, id: 'reply-uuid-3', from: 'kat', to: 'vec',
      kind: 'result', in_reply_to: publishedEnv.id,
      ts: '2026-08-27T00:00:00.000Z', payload: { done: true },
    }
    await bus.handleResult(reply)
    await promise

    const matchedEntry = readAudit(auditLogPath).find(
      e => e.dir === 'in' && e.envelope_id === 'reply-uuid-3',
    )
    expect(matchedEntry).toBeDefined()
    expect(matchedEntry!.note).toBe('ledger_matched')
    // THIS is the round-3 P2: the matched-reply's inbound audit must carry
    // the original request's req_id so operators can correlate reply-to-request.
    expect(matchedEntry!.req_id).toBe(requestReqId)
  })

  test('ledger_overflow drop audit carries the evicted request req_id', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'fleet-audit-'))
    const auditLogPath = join(dir, 'fleet-bus.jsonl')
    const nc = new FakeNatsConnection()
    const bus = new TestFleetBus({
      botName: 'vec', url: 'nats://unused', user: 'vec', password: 'unused', auditLogPath,
      inflightLedgerCap: 1,
    }, allowlist)
    bus.attachFakeNc(nc)
    const p1 = bus.request({ to: 'kat', kind: 'text_message', payload: { i: 1 }, wait: true, timeoutMs: 5_000 })
    const p2 = bus.request({ to: 'kat', kind: 'text_message', payload: { i: 2 }, wait: true, timeoutMs: 5_000 })
    void p2.then(() => {}, () => {})
    const r1 = await p1
    expect(r1.error).toBe('ledger_overflow')
    const overflowEntry = readAudit(auditLogPath).find(e => e.reason === 'ledger_overflow')
    expect(overflowEntry).toBeDefined()
    expect(typeof overflowEntry!.req_id).toBe('string')
    expect(overflowEntry!.envelope_id).toBe(r1.envelope!.id)
  })

  test('successful inject writes exactly ONE dir:in audit per received envelope (round-3 dedup)', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'fleet-audit-'))
    const auditLogPath = join(dir, 'fleet-bus.jsonl')
    const events: FleetBusSessionEvent[] = []
    const bus = new TestFleetBus({
      botName: 'vec', url: 'nats://unused', user: 'vec', password: 'unused', auditLogPath,
      injectIntoSession: async event => { events.push(event) },
    }, allowlist)
    await bus.handleRequest(envelope({ from: 'ohm', id: 'wire-dedup-1' }))
    expect(events).toHaveLength(1)
    const inEntries = readAudit(auditLogPath).filter(e => e.dir === 'in' && e.envelope_id === 'wire-dedup-1')
    // Previously this was 2 (one from the caller, one from injectIntoSession).
    expect(inEntries).toHaveLength(1)
  })

  test('unsolicited reply successful inject writes exactly ONE dir:in audit (round-3 dedup)', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'fleet-audit-'))
    const auditLogPath = join(dir, 'fleet-bus.jsonl')
    const nc = new FakeNatsConnection()
    const events: FleetBusSessionEvent[] = []
    const bus = new TestFleetBus({
      botName: 'vec', url: 'nats://unused', user: 'vec', password: 'unused', auditLogPath,
      injectIntoSession: async event => { events.push(event) },
    }, allowlist)
    bus.attachFakeNc(nc)
    const reply = {
      envelope_version: 1, id: 'wire-dedup-unsol-1', from: 'kat', to: 'vec',
      kind: 'result', in_reply_to: 'never-was-in-flight',
      ts: '2026-08-27T00:00:00.000Z', payload: {},
    }
    await bus.handleResult(reply)
    expect(events).toHaveLength(1)
    expect(events[0]!.unsolicited).toBe(true)
    const inEntries = readAudit(auditLogPath).filter(
      e => e.dir === 'in' && e.envelope_id === 'wire-dedup-unsol-1',
    )
    // Exactly ONE audit entry, carrying the unsolicited_reply note.
    expect(inEntries).toHaveLength(1)
    expect(inEntries[0]!.note).toBe('unsolicited_reply')
  })
})

describe('onResult inject path', () => {
  test('recipient mismatch audits misrouted_reply and discards', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'fleet-audit-'))
    const auditLogPath = join(dir, 'fleet-bus.jsonl')
    const nc = new FakeNatsConnection()
    const events: FleetBusSessionEvent[] = []
    const bus = new TestFleetBus({
      botName: 'vec', url: 'nats://unused', user: 'vec', password: 'unused', auditLogPath,
      injectIntoSession: async event => { events.push(event) },
    }, allowlist)
    bus.attachFakeNc(nc)
    const reply = {
      envelope_version: 1, id: 'r-99', from: 'kat', to: 'ohm', // <-- addressed to ohm, not vec
      kind: 'result', in_reply_to: 'x', ts: '2026-08-27T00:00:00.000Z', payload: {},
    }
    await bus.handleResult(reply)
    expect(events).toHaveLength(0)
    expect(readAudit(auditLogPath).some(e => e.reason === 'misrouted_reply')).toBe(true)
  })

  test('from mismatch audits reply_from_mismatch, injects as unsolicited, does not resolve waiter', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'fleet-audit-'))
    const auditLogPath = join(dir, 'fleet-bus.jsonl')
    const nc = new FakeNatsConnection()
    const events: FleetBusSessionEvent[] = []
    const bus = new TestFleetBus({
      botName: 'vec', url: 'nats://unused', user: 'vec', password: 'unused', auditLogPath,
      injectIntoSession: async event => { events.push(event) },
    }, allowlist)
    bus.attachFakeNc(nc)

    const promise = bus.request({ to: 'kat', kind: 'text_message', payload: {}, wait: true, timeoutMs: 5_000 })
    await Promise.resolve()
    const publishedEnv = nc.publishes[0]!.envelope as Envelope

    let resolved = false
    void promise.then(() => { resolved = true })

    const reply = {
      envelope_version: 1, id: 'r-100', from: 'ohm', to: 'vec', // <-- expected kat, got ohm
      kind: 'result', in_reply_to: publishedEnv.id, ts: '2026-08-27T00:00:00.000Z', payload: {},
    }
    await bus.handleResult(reply)
    // Waiter must NOT resolve.
    await Promise.resolve()
    expect(resolved).toBe(false)
    // Injected as unsolicited.
    expect(events).toHaveLength(1)
    expect(events[0]!.unsolicited).toBe(true)
    // Audit contains reply_from_mismatch.
    expect(readAudit(auditLogPath).some(e => e.reason === 'reply_from_mismatch')).toBe(true)
  })

  test('unsolicited reply (no ledger match, no evicted match) injects with no lateReplyEnvId', async () => {
    const nc = new FakeNatsConnection()
    const events: FleetBusSessionEvent[] = []
    const bus = new TestFleetBus({
      botName: 'vec', url: 'nats://unused', user: 'vec', password: 'unused',
      injectIntoSession: async event => { events.push(event) },
    }, allowlist)
    bus.attachFakeNc(nc)
    const reply = {
      envelope_version: 1, id: 'r-101', from: 'kat', to: 'vec',
      kind: 'result', in_reply_to: 'never-was-in-flight', ts: '2026-08-27T00:00:00.000Z', payload: {},
    }
    await bus.handleResult(reply)
    expect(events).toHaveLength(1)
    expect(events[0]!.unsolicited).toBe(true)
    expect(events[0]!.lateReplyEnvId).toBeUndefined()
  })

  test('late reply (in evicted ledger) injects with lateReplyEnvId', async () => {
    const nc = new FakeNatsConnection()
    const events: FleetBusSessionEvent[] = []
    const bus = new TestFleetBus({
      botName: 'vec', url: 'nats://unused', user: 'vec', password: 'unused',
      injectIntoSession: async event => { events.push(event) },
    }, allowlist)
    bus.attachFakeNc(nc)
    // Fire a wait request that times out fast.
    const first = await bus.request({ to: 'kat', kind: 'text_message', payload: {}, wait: true, timeoutMs: 20 })
    expect(first.timed_out).toBe(true)
    const originalId = first.envelope!.id
    expect(bus.evictedLedgerHas(originalId)).toBe(true)

    const reply = {
      envelope_version: 1, id: 'r-late-1', from: 'kat', to: 'vec',
      kind: 'result', in_reply_to: originalId, ts: '2026-08-27T00:00:00.000Z', payload: {},
    }
    await bus.handleResult(reply)
    expect(events).toHaveLength(1)
    expect(events[0]!.unsolicited).toBe(true)
    expect(events[0]!.lateReplyEnvId).toBe(originalId)
  })
})

/* -------------------------------------------------------------------------- */
/* Rate limiter wiring                                                         */
/* -------------------------------------------------------------------------- */

class DenyBucket implements TokenBucket { allow(): boolean { return false } }
class AllowBucket implements TokenBucket { allow(): boolean { return true } }

describe('rate limiter wiring', () => {
  test('perFrom deny audits rate_limited_per_from and skips inject on request', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'fleet-audit-'))
    const auditLogPath = join(dir, 'fleet-bus.jsonl')
    const events: FleetBusSessionEvent[] = []
    const bus = new TestFleetBus({
      botName: 'vec', url: 'nats://unused', user: 'vec', password: 'unused', auditLogPath,
      rateLimiters: {
        perFrom: new DenyBucket(),
        perSubject: new AllowBucket(),
        perSessionInject: new AllowBucket(),
      },
      injectIntoSession: async e => { events.push(e) },
    }, allowlist)
    await bus.handleRequest(envelope({ from: 'ohm' }))
    expect(events).toHaveLength(0)
    expect(readAudit(auditLogPath).some(e => e.reason === 'claude_discord_adapter_rate_limited_per_from')).toBe(true)
  })

  test('perSubject deny audits rate_limited_per_subject', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'fleet-audit-'))
    const auditLogPath = join(dir, 'fleet-bus.jsonl')
    const events: FleetBusSessionEvent[] = []
    const bus = new TestFleetBus({
      botName: 'vec', url: 'nats://unused', user: 'vec', password: 'unused', auditLogPath,
      rateLimiters: {
        perFrom: new AllowBucket(),
        perSubject: new DenyBucket(),
        perSessionInject: new AllowBucket(),
      },
      injectIntoSession: async e => { events.push(e) },
    }, allowlist)
    await bus.handleRequest(envelope({ from: 'ohm' }))
    expect(events).toHaveLength(0)
    expect(readAudit(auditLogPath).some(e => e.reason === 'claude_discord_adapter_rate_limited_per_subject')).toBe(true)
  })

  test('perSessionInject deny audits and skips inject on request', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'fleet-audit-'))
    const auditLogPath = join(dir, 'fleet-bus.jsonl')
    const events: FleetBusSessionEvent[] = []
    const bus = new TestFleetBus({
      botName: 'vec', url: 'nats://unused', user: 'vec', password: 'unused', auditLogPath,
      rateLimiters: {
        perFrom: new AllowBucket(),
        perSubject: new AllowBucket(),
        perSessionInject: new DenyBucket(),
      },
      injectIntoSession: async e => { events.push(e) },
    }, allowlist)
    await bus.handleRequest(envelope({ from: 'ohm' }))
    expect(events).toHaveLength(0)
    expect(readAudit(auditLogPath).some(e => e.reason === 'claude_discord_adapter_rate_limited_per_session_inject')).toBe(true)
  })

  test('ledger-matched .result BYPASSES perSessionInject and resolves waiter', async () => {
    const nc = new FakeNatsConnection()
    const events: FleetBusSessionEvent[] = []
    const bus = new TestFleetBus({
      botName: 'vec', url: 'nats://unused', user: 'vec', password: 'unused',
      rateLimiters: {
        perFrom: new AllowBucket(),
        perSubject: new AllowBucket(),
        perSessionInject: new DenyBucket(), // would block a session inject, but ledger match bypasses
      },
      injectIntoSession: async e => { events.push(e) },
    }, allowlist)
    bus.attachFakeNc(nc)
    const promise = bus.request({ to: 'kat', kind: 'text_message', payload: {}, wait: true, timeoutMs: 5_000 })
    await Promise.resolve()
    const publishedEnv = nc.publishes[0]!.envelope as Envelope
    const reply = {
      envelope_version: 1, id: 'r-200', from: 'kat', to: 'vec',
      kind: 'result', in_reply_to: publishedEnv.id, ts: '2026-08-27T00:00:00.000Z', payload: {},
    }
    await bus.handleResult(reply)
    const result = await promise
    expect(result.ok).toBe(true)
    expect(result.reply?.id).toBe('r-200')
    expect(events).toHaveLength(0)
  })

  test('unsolicited reply IS gated by perSessionInject deny', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'fleet-audit-'))
    const auditLogPath = join(dir, 'fleet-bus.jsonl')
    const nc = new FakeNatsConnection()
    const events: FleetBusSessionEvent[] = []
    const bus = new TestFleetBus({
      botName: 'vec', url: 'nats://unused', user: 'vec', password: 'unused', auditLogPath,
      rateLimiters: {
        perFrom: new AllowBucket(),
        perSubject: new AllowBucket(),
        perSessionInject: new DenyBucket(),
      },
      injectIntoSession: async e => { events.push(e) },
    }, allowlist)
    bus.attachFakeNc(nc)
    const reply = {
      envelope_version: 1, id: 'r-201', from: 'kat', to: 'vec',
      kind: 'result', in_reply_to: 'unmatched', ts: '2026-08-27T00:00:00.000Z', payload: {},
    }
    await bus.handleResult(reply)
    expect(events).toHaveLength(0)
    expect(readAudit(auditLogPath).some(e => e.reason === 'claude_discord_adapter_rate_limited_per_session_inject')).toBe(true)
  })
})

describe('FixedWindowBucket', () => {
  test('allows up to capacity then denies within window', () => {
    let now = 0
    const b = new FixedWindowBucket(3, 1_000, () => now)
    expect(b.allow('k')).toBe(true)
    expect(b.allow('k')).toBe(true)
    expect(b.allow('k')).toBe(true)
    expect(b.allow('k')).toBe(false)
    now = 1_001
    expect(b.allow('k')).toBe(true)
  })

  test('separate keys have separate windows', () => {
    let now = 0
    const b = new FixedWindowBucket(1, 1_000, () => now)
    expect(b.allow('a')).toBe(true)
    expect(b.allow('a')).toBe(false)
    expect(b.allow('b')).toBe(true)
  })
})

/* -------------------------------------------------------------------------- */
/* Supervisor + publish-only mode                                              */
/* -------------------------------------------------------------------------- */

describe('publish-only mode', () => {
  test('connect() skips subscriptions and heartbeat', async () => {
    const nc = new FakeNatsConnection()
    const bus = new FleetBus({
      botName: 'vec', url: 'nats://unused', user: 'vec', password: 'unused',
      mode: 'publish-only',
      connectFn: async () => nc as unknown as NatsConnection,
    }, allowlist)
    await bus.connect()
    expect(nc.subscribed).toHaveLength(0)
    // No heartbeat was published either.
    expect(nc.publishes).toHaveLength(0)
    await bus.disconnect()
  })

  test('publishReply returns multi_instance_publish_only', async () => {
    const nc = new FakeNatsConnection()
    const bus = new FleetBus({
      botName: 'vec', url: 'nats://unused', user: 'vec', password: 'unused',
      mode: 'publish-only',
      connectFn: async () => nc as unknown as NatsConnection,
    }, allowlist)
    await bus.connect()
    const result = bus.publishReply('any', {})
    expect(result.ok).toBe(false)
    expect(result.error).toBe('multi_instance_publish_only')
    await bus.disconnect()
  })

  test('request({wait:true}) returns multi_instance_publish_only', async () => {
    const nc = new FakeNatsConnection()
    const bus = new FleetBus({
      botName: 'vec', url: 'nats://unused', user: 'vec', password: 'unused',
      mode: 'publish-only',
      connectFn: async () => nc as unknown as NatsConnection,
    }, allowlist)
    await bus.connect()
    const result = await bus.request({ to: 'kat', kind: 'text_message', payload: {}, wait: true })
    expect(result.ok).toBe(false)
    expect(result.error).toBe('multi_instance_publish_only')
    await bus.disconnect()
  })

  test('request({wait:false}) still publishes in publish-only mode', async () => {
    const nc = new FakeNatsConnection()
    const bus = new FleetBus({
      botName: 'vec', url: 'nats://unused', user: 'vec', password: 'unused',
      mode: 'publish-only',
      connectFn: async () => nc as unknown as NatsConnection,
    }, allowlist)
    await bus.connect()
    const result = await bus.request({ to: 'kat', kind: 'text_message', payload: {} })
    expect(result.ok).toBe(true)
    expect(nc.publishes).toHaveLength(1)
    expect(nc.publishes[0]!.subject).toBe('fleet.kat.request')
    await bus.disconnect()
  })
})

describe('supervisor loop', () => {
  test('reconnects after the underlying NATS connection closes', async () => {
    const connections: FakeNatsConnection[] = []
    const bus = new FleetBus({
      botName: 'vec', url: 'nats://unused', user: 'vec', password: 'unused',
      supervisorSleepMs: 10,
      connectFn: async () => {
        const nc = new FakeNatsConnection()
        connections.push(nc)
        return nc as unknown as NatsConnection
      },
    }, allowlist)
    const done = bus.run()
    // Wait for first connection to establish.
    await new Promise(r => setTimeout(r, 40))
    expect(connections.length).toBe(1)
    // Simulate CLOSED — supervisor should call disconnect + reconnect.
    connections[0]!.markClosed()
    await new Promise(r => setTimeout(r, 60))
    expect(connections.length).toBeGreaterThanOrEqual(2)
    await bus.stop()
    await done
  })

  test('runSupervisor helper returns bus + done promise', async () => {
    const { bus, done } = runSupervisor({
      botName: 'vec', url: 'nats://unused', user: 'vec', password: 'unused',
      supervisorSleepMs: 10,
      connectFn: async () => new FakeNatsConnection() as unknown as NatsConnection,
    }, allowlist)
    await new Promise(r => setTimeout(r, 30))
    await bus.stop()
    await done
  })

  test('rechecks supervisorStopping after a slow connect resolves', async () => {
    // Race: stop() fires while connect() is in flight. The supervisor must
    // recheck stopping BEFORE waitClosed, or it hangs on a fresh connection
    // the caller thought was torn down.
    let releaseConnect: (() => void) | null = null
    const connections: FakeNatsConnection[] = []
    const bus = new FleetBus({
      botName: 'vec', url: 'nats://unused', user: 'vec', password: 'unused',
      supervisorSleepMs: 10,
      connectFn: async () => {
        const isFirst = connections.length === 0
        const nc = new FakeNatsConnection()
        connections.push(nc)
        if (isFirst) {
          await new Promise<void>(r => { releaseConnect = r })
        }
        return nc as unknown as NatsConnection
      },
    }, allowlist)
    const done = bus.run()
    // Let connectFn begin blocking.
    await new Promise(r => setTimeout(r, 10))
    expect(releaseConnect).not.toBeNull()
    // Fire stop() while connect() is still in flight.
    const stopP = bus.stop()
    // Give stop() a tick to set supervisorStopping = true before we release connect.
    await Promise.resolve()
    releaseConnect!()
    await Promise.all([stopP, done])
    // Supervisor exited after the first connect returned; no reconnect loop.
    expect(connections.length).toBe(1)
  })
})

const integrationTest = process.env.FLEET_BUS_INTEGRATION === '1' ? test : test.skip

describe('NATS authorization boundary', () => {
  integrationTest('console publish is rejected and never delivered', async () => {
    const server = process.env.FLEET_BUS_URL ?? 'nats://nats:4222'
    const tokenDir = process.env.FLEET_BUS_TOKEN_DIR ?? join(homedir(), '.claude')
    const luna = await connect({
      servers: server,
      user: 'luna',
      pass: readFileSync(join(tokenDir, 'fleet-bus-token-luna'), 'utf8').trim(),
      inboxPrefix: '_INBOX_luna',
    })
    const consoleClient = await connect({
      servers: server,
      user: 'console',
      pass: readFileSync(join(tokenDir, 'fleet-bus-token-console'), 'utf8').trim(),
      inboxPrefix: '_INBOX_console',
    })

    try {
      let delivered = false
      const subscription = luna.subscribe('fleet.luna.request', {
        callback: () => { delivered = true },
      })
      await luna.flush()

      const violations: string[] = []
      void (async () => {
        for await (const status of consoleClient.status()) {
          if (status.type === 'error' || status.type === 'permissionError') {
            violations.push(String(status.data))
          }
        }
      })().catch(() => {})

      consoleClient.publish('fleet.luna.request', JSONCodec().encode(envelope({ from: 'console' })))
      await consoleClient.flush()
      await new Promise(resolve => setTimeout(resolve, 500))

      expect(violations.some(value => /permissions?[_ ]violation/i.test(value))).toBe(true)
      expect(delivered).toBe(false)
      subscription.unsubscribe()
    } finally {
      await Promise.all([consoleClient.close(), luna.close()])
    }
  })
})
