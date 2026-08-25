import { describe, expect, test } from 'bun:test'
import { connect, JSONCodec, type Msg } from 'nats'
import { mkdtempSync, readFileSync, writeFileSync } from 'node:fs'
import { homedir, tmpdir } from 'node:os'
import { join } from 'node:path'
import {
  DEFAULT_MAX_ENVELOPE_BYTES,
  createHeartbeatEnvelope,
  FleetBus,
  buildFleetBusFrameMeta,
  loadFleetManifestAllowlist,
  normalizeAllowlist,
  normalizeBotName,
  validateEnvelope,
} from './fleet-bus'

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
    expect(normalizeBotName('k\u200Bat')).toBeNull()
    expect(normalizeBotName(' kat')).toBeNull()
    expect(normalizeBotName('kat.bot')).toBeNull()
  })

  test('normalizes, deduplicates, and rejects invalid manifest entries', () => {
    expect([...normalizeAllowlist(['VEC', 'vec', 'myc'])]).toEqual(['vec', 'myc'])
    expect(() => normalizeAllowlist(['valid', 'not valid'])).toThrow(TypeError)
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
    ['missing payload', (() => { const value = envelope(); delete value.payload; return value })(), 'missing_payload'],
    ['unknown sender', envelope({ from: 'fernando' }), 'from_claim_rejected'],
    ['homoglyph sender', envelope({ from: 'оhm' }), 'from_claim_rejected'],
  ])('rejects %s', (_label, value, error) => {
    expect(validateEnvelope(value, allowlist)).toEqual({ ok: false, error })
  })

  test('rejects envelopes above the encoded byte limit', () => {
    const value = envelope({ payload: 'x'.repeat(DEFAULT_MAX_ENVELOPE_BYTES) })
    expect(validateEnvelope(value, allowlist)).toEqual({ ok: false, error: 'envelope_too_large' })
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

class TestFleetBus extends FleetBus {
  handleRequest(value: unknown): Promise<void> {
    return this.onRequest({
      subject: 'fleet.vec.request',
      data: JSONCodec().encode(value),
    } as Msg)
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
    expect(events[0].envelope.from).toBe('ohm')
    expect(events[0].reqId).toMatch(/^[a-f0-9]{32}$/)
    expect(events[0].reqId).not.toBe(value.id)
  })

  test('frame metadata exposes both the server nonce and wire envelope id', () => {
    const value = envelope()
    expect(buildFleetBusFrameMeta({ envelope: value, reqId: 'a'.repeat(32) })).toEqual({
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
