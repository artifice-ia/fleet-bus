import {
  connect as natsConnect,
  JSONCodec,
  type ConnectionOptions,
  type NatsConnection,
  type Msg,
  type Subscription,
} from 'nats'
import { randomBytes, randomUUID } from 'node:crypto'
import { appendFileSync, chmodSync, mkdirSync, readFileSync } from 'node:fs'
import { dirname } from 'node:path'
import { parse as parseYaml } from 'yaml'

export const DEFAULT_MAX_ENVELOPE_BYTES = 1_044_480
export const DEFAULT_INFLIGHT_LEDGER_CAP = 1000
export const DEFAULT_RECEIVE_LEDGER_CAP = 1000
export const DEFAULT_EVICTED_LEDGER_CAP = 1000
export const DEFAULT_REQUEST_TIMEOUT_MS = 30_000
export const DEFAULT_ATTR_MAX_LEN = 1024
export const DEFAULT_PAYLOAD_BODY_MAX_BYTES = 8192

export interface Envelope<P = unknown> {
  envelope_version: 1
  id: string
  from: string
  to?: string | null
  kind: string
  in_reply_to?: string
  ts: string
  payload: P
  root_id?: string
  origin?: string
  owner?: string
  hops?: number
}

export type FleetBusMode = 'primary' | 'publish-only'

export interface FleetBusConfig {
  botName: string
  url: string
  user: string
  password: string
  subscribeBroadcast?: boolean
  maxEnvelopeBytes?: number
  heartbeatIntervalMs?: number
  pluginVersion?: string
  logger?: (message: string) => void
  auditLogPath?: string
  injectIntoSession?: (event: FleetBusSessionEvent) => Promise<void>
  mode?: FleetBusMode
  inflightLedgerCap?: number
  receiveLedgerCap?: number
  evictedLedgerCap?: number
  rateLimiters?: FleetBusRateLimiters
  supervisorSleepMs?: number
  /**
   * Injected NATS connect function for testing. Defaults to `nats.connect`.
   * Signature intentionally loose to match the module's exported type.
   */
  connectFn?: (options: ConnectionOptions) => Promise<NatsConnection>
}

export interface FleetBusSessionEvent {
  envelope: Envelope
  reqId: string
  /** Set on `.result` envelopes that did not match an outstanding request. */
  unsolicited?: boolean
  /** Set when this envelope is a late reply to a request whose waiter was already evicted. */
  lateReplyEnvId?: string
}

export interface FleetBusFrameMeta {
  source: 'fleet-bus'
  authenticated: 'false'
  from_claim: string
  kind: string
  req_id: string
  env_id: string
  ts: string
  late_reply_env_id?: string
}

export interface TokenBucket {
  allow(key: string): boolean
}

export interface FleetBusRateLimiters {
  perFrom: TokenBucket
  perSubject: TokenBucket
  perSessionInject: TokenBucket
}

export interface FleetBusRequestOptions {
  to: string
  kind: string
  payload: unknown
  wait?: boolean
  timeoutMs?: number
  force?: boolean
  /** Baton lineage source — the inbound envelope currently being answered. Omit for originating requests. */
  inbound?: Envelope
}

export interface FleetBusRequestResult {
  ok: boolean
  envelope?: Envelope
  delivered_to_subscriber?: boolean
  error?: string
  reply?: Envelope
  timed_out?: boolean
}

export interface FleetBusReplyResult {
  ok: boolean
  envelope?: Envelope
  error?: 'req_id_unknown' | 'multi_instance_publish_only' | string
  req_id?: string
}

export type EnvelopeValidationResult =
  | { ok: true; envelope: Envelope }
  | { ok: false; error: string }

const BOT_NAME_PATTERN = /^[a-z0-9_-]+$/
export const BROADCAST_KIND_RE = /^[a-z0-9_-]+(\.[a-z0-9_-]+)*$/
export const RESERVED_BOT_NAMES = new Set(['broadcast'])

/** Return the canonical bus identity, or null for a non-ASCII/invalid/reserved claim. */
export function normalizeBotName(value: unknown): string | null {
  if (typeof value !== 'string') return null
  const normalized = value.normalize('NFKC').toLowerCase()
  if (!BOT_NAME_PATTERN.test(normalized)) return null
  if (RESERVED_BOT_NAMES.has(normalized)) return null
  return normalized
}

/** Normalize a manifest bot_names list, rejecting invalid entries. */
export function normalizeAllowlist(values: Iterable<unknown>): Set<string> {
  const result = new Set<string>()
  for (const value of values) {
    const normalized = normalizeBotName(value)
    if (normalized === null) throw new TypeError(`Invalid fleet bot name: ${String(value)}`)
    result.add(normalized)
  }
  return result
}

export function loadFleetManifestAllowlist(path: string): Set<string> {
  const manifest = parseYaml(readFileSync(path, 'utf8')) as unknown
  if (typeof manifest !== 'object' || manifest === null || Array.isArray(manifest)) {
    throw new TypeError('Fleet manifest must be a YAML mapping')
  }
  const botNames = (manifest as Record<string, unknown>).bot_names
  if (!Array.isArray(botNames) || botNames.length === 0) {
    throw new TypeError('Fleet manifest bot_names must be a non-empty list')
  }
  return normalizeAllowlist(botNames)
}

export function createHeartbeatEnvelope(
  botName: string,
  pluginVersion: string,
  pid = process.pid,
  now = new Date(),
): Envelope {
  const from = normalizeBotName(botName)
  if (from === null) throw new TypeError('Invalid heartbeat bot name')
  const ts = now.toISOString()
  return {
    envelope_version: 1,
    id: randomUUID(),
    from,
    to: null,
    kind: 'status_heartbeat',
    ts,
    payload: {
      online: true,
      process_alive_ts: ts,
      session_last_response_ts: null,
      injection_delivered_ts: null,
      pid,
      plugin_version: pluginVersion,
    },
  }
}

/**
 * Validate the v1 wire envelope before it reaches any bus handler.
 * Extended in Stage 3 to enforce baton-value validity on the wire.
 */
export function validateEnvelope(
  value: unknown,
  allowedFromClaims: ReadonlySet<string>,
  maxBytes = DEFAULT_MAX_ENVELOPE_BYTES,
): EnvelopeValidationResult {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    return { ok: false, error: 'envelope_not_object' }
  }

  const candidate = value as Record<string, unknown>
  if (candidate.envelope_version !== 1) return { ok: false, error: 'unsupported_envelope_version' }
  if (typeof candidate.id !== 'string' || candidate.id.length === 0) return { ok: false, error: 'invalid_id' }
  if (typeof candidate.kind !== 'string' || candidate.kind.length === 0) return { ok: false, error: 'invalid_kind' }
  if (typeof candidate.ts !== 'string' || Number.isNaN(Date.parse(candidate.ts))) return { ok: false, error: 'invalid_ts' }
  if (!Object.hasOwn(candidate, 'payload')) return { ok: false, error: 'missing_payload' }
  if (candidate.to !== undefined && candidate.to !== null && typeof candidate.to !== 'string') {
    return { ok: false, error: 'invalid_to' }
  }
  if (candidate.in_reply_to !== undefined && typeof candidate.in_reply_to !== 'string') {
    return { ok: false, error: 'invalid_in_reply_to' }
  }

  const from = normalizeBotName(candidate.from)
  if (from === null || !allowedFromClaims.has(from)) return { ok: false, error: 'from_claim_rejected' }

  // Baton-value validation (v1.x additive; discipline ported from bus.py::validate_envelope).
  if (Object.hasOwn(candidate, 'root_id')) {
    if (typeof candidate.root_id !== 'string' || candidate.root_id.length === 0) {
      return { ok: false, error: 'claude_discord_adapter_invalid_root_id' }
    }
  }
  for (const field of ['origin', 'owner'] as const) {
    if (Object.hasOwn(candidate, field)) {
      const claim = candidate[field]
      const canonical = normalizeBotName(claim)
      if (canonical === null || canonical !== claim || !allowedFromClaims.has(canonical)) {
        return { ok: false, error: `claude_discord_adapter_invalid_${field}` }
      }
    }
  }
  if (Object.hasOwn(candidate, 'hops')) {
    const hops = candidate.hops
    if (!Number.isInteger(hops) || (hops as number) < 0) {
      return { ok: false, error: 'claude_discord_adapter_invalid_hops' }
    }
  }

  let encodedBytes: number
  try {
    encodedBytes = Buffer.byteLength(JSON.stringify(candidate), 'utf8')
  } catch {
    return { ok: false, error: 'payload_not_serializable' }
  }
  if (encodedBytes > maxBytes) return { ok: false, error: 'envelope_too_large' }

  return { ok: true, envelope: { ...candidate, from } as unknown as Envelope }
}

/* -------------------------------------------------------------------------- */
/* Baton derivation                                                            */
/* -------------------------------------------------------------------------- */

export class BatonDerivationError extends Error {
  readonly reason: string
  constructor(reason: string, message: string) {
    super(message)
    this.reason = reason
    this.name = 'BatonDerivationError'
  }
}

export class BatonHopsExhausted extends BatonDerivationError {
  constructor(message = 'baton hop ceiling reached') {
    super('claude_discord_adapter_baton_hops_exhausted', message)
    this.name = 'BatonHopsExhausted'
  }
}

export interface BatonFields {
  root_id: string
  origin: string
  owner: string
  hops: number
}

export interface DeriveBatonInput {
  envelopeId: string
  botName: string
  inbound?: Envelope
  kind: string
  recipient?: string | null
}

/**
 * Derive baton fields for an outbound envelope. Port of
 * `codex-container/bus.py::derive_baton_fields` — port the shape, not the lines.
 * Baton discipline is settled after three review rounds; do not redesign.
 */
export function deriveBatonFields(input: DeriveBatonInput): BatonFields {
  const canonicalBot = normalizeBotName(input.botName)
  if (canonicalBot === null) {
    throw new BatonDerivationError(
      'claude_discord_adapter_baton_bot_name_invalid',
      `invalid baton origin bot name: ${String(input.botName)}`,
    )
  }
  const { envelopeId, inbound, kind, recipient } = input
  let fields: BatonFields
  if (inbound === undefined) {
    fields = { root_id: envelopeId, origin: canonicalBot, owner: canonicalBot, hops: 0 }
  } else {
    const inboundRecord = inbound as unknown as Record<string, unknown>
    const rawRootId = inboundRecord.root_id ?? inboundRecord.id
    const rawOrigin = inboundRecord.origin ?? inboundRecord.from
    const rawOwner = inboundRecord.owner ?? inboundRecord.from
    const rawHops = inboundRecord.hops
    // Number.isInteger deliberately (typeof === 'number' would accept NaN).
    const prevHops = Number.isInteger(rawHops) ? (rawHops as number) : 0
    fields = {
      root_id: rawRootId as string,
      origin: rawOrigin as string,
      owner: rawOwner as string,
      hops: prevHops + 1,
    }
  }
  if (kind === 'baton.handoff') {
    const canonicalRecipient = normalizeBotName(recipient)
    if (canonicalRecipient === null) {
      throw new BatonDerivationError(
        'claude_discord_adapter_baton_handoff_recipient_invalid',
        'baton handoff requires a direct canonical recipient',
      )
    }
    fields.owner = canonicalRecipient
  }
  if (fields.hops >= 16) {
    throw new BatonHopsExhausted()
  }
  return fields
}

/* -------------------------------------------------------------------------- */
/* Frame escape / caps                                                         */
/* -------------------------------------------------------------------------- */

const XML_ESCAPE_MAP: Record<string, string> = {
  '&': '&amp;',
  '<': '&lt;',
  '>': '&gt;',
  '"': '&quot;',
  "'": '&apos;',
}
/** Zero-width and bidi-control characters — parity with bus.py `_ZERO_WIDTH_RE`. */
const ZERO_WIDTH_RE = /[\u200b-\u200f\u202a-\u202e\u2060-\u206f\ufeff]/g

function xmlEscape(value: string): string {
  return value.replace(/[&<>"']/g, char => XML_ESCAPE_MAP[char] ?? char)
}

/**
 * Sanitize an identifier field for use as an XML attribute value.
 * Identifier fields (env_id, req_id, root_id) are XML-escaped but NOT
 * zero-width stripped — they must remain byte-identical for audit
 * correlation across the fleet.
 */
export function escapeFrameIdentifier(value: unknown, maxLen = DEFAULT_ATTR_MAX_LEN): string {
  const escaped = xmlEscape(String(value))
  if (escaped.length > maxLen) throw new RangeError(`frame identifier exceeds ${maxLen} chars (${escaped.length})`)
  return escaped
}

/**
 * Sanitize a human-visible content field for use as an XML attribute value.
 * Zero-width characters are stripped BEFORE escaping to prevent hidden
 * injection surface from RTL overrides or invisible spacers.
 */
export function escapeFrameContent(value: unknown, maxLen = DEFAULT_ATTR_MAX_LEN): string {
  const stripped = String(value).replace(ZERO_WIDTH_RE, '')
  const escaped = xmlEscape(stripped)
  if (escaped.length > maxLen) throw new RangeError(`frame content exceeds ${maxLen} chars (${escaped.length})`)
  return escaped
}

/** JSON-encode a payload for the frame body with 8KB cap + truncation marker. */
export function buildFleetBusFramePayloadBody(
  envelope: Envelope,
  maxBytes = DEFAULT_PAYLOAD_BODY_MAX_BYTES,
): { body: string; truncated: boolean } {
  const encoded = JSON.stringify(envelope.payload) ?? 'null'
  const size = Buffer.byteLength(encoded, 'utf8')
  if (size <= maxBytes) return { body: xmlEscape(encoded), truncated: false }
  // env_id is XML-escaped so a hostile id can't break out of the <payload> tag.
  const marker = `\n[...truncated 8KB max, full envelope in audit log env_id=${xmlEscape(String(envelope.id))}]`
  const truncated = Buffer.from(encoded, 'utf8').subarray(0, maxBytes).toString('utf8')
  return { body: xmlEscape(truncated) + marker, truncated: true }
}

export function buildFleetBusFrameMeta(event: FleetBusSessionEvent): FleetBusFrameMeta {
  const { envelope, reqId, lateReplyEnvId } = event
  const meta: FleetBusFrameMeta = {
    source: 'fleet-bus',
    authenticated: 'false',
    from_claim: escapeFrameContent(envelope.from),
    kind: escapeFrameContent(envelope.kind),
    req_id: escapeFrameIdentifier(reqId),
    env_id: escapeFrameIdentifier(envelope.id),
    ts: escapeFrameIdentifier(envelope.ts),
  }
  if (lateReplyEnvId !== undefined) {
    meta.late_reply_env_id = escapeFrameIdentifier(lateReplyEnvId)
  }
  return meta
}

/** Build a complete `<channel>` injection frame for the given session event. */
export function buildFleetBusFrame(event: FleetBusSessionEvent): string {
  const meta = buildFleetBusFrameMeta(event)
  const { envelope } = event
  const attrParts = [
    `source="${meta.source}"`,
    `authenticated="${meta.authenticated}"`,
    `from_claim="${meta.from_claim}"`,
    `kind="${meta.kind}"`,
    `env_id="${meta.env_id}"`,
    `req_id="${meta.req_id}"`,
    `ts="${meta.ts}"`,
  ]
  if (meta.late_reply_env_id !== undefined) {
    attrParts.push(`late_reply_env_id="${meta.late_reply_env_id}"`)
  }
  for (const field of ['root_id', 'origin', 'owner', 'hops'] as const) {
    const value = envelope[field]
    if (value === undefined || value === null) continue
    attrParts.push(`${field}="${escapeFrameIdentifier(value)}"`)
  }
  const { body } = buildFleetBusFramePayloadBody(envelope)
  return `<channel ${attrParts.join(' ')}>\n<payload>${body}</payload>\n</channel>`
}

/* -------------------------------------------------------------------------- */
/* Default rate limiter (fixed-window per key)                                 */
/* -------------------------------------------------------------------------- */

/** Simple per-key fixed-window rate limiter. Sufficient for the 30/min-class limits. */
export class FixedWindowBucket implements TokenBucket {
  private readonly windows = new Map<string, { count: number; expiresAt: number }>()
  constructor(
    public readonly capacity: number,
    public readonly windowMs: number,
    private readonly clock: () => number = () => Date.now(),
  ) {}

  allow(key: string): boolean {
    const now = this.clock()
    const window = this.windows.get(key)
    if (window === undefined || window.expiresAt <= now) {
      this.windows.set(key, { count: 1, expiresAt: now + this.windowMs })
      return true
    }
    if (window.count >= this.capacity) return false
    window.count += 1
    return true
  }
}

function envInt(name: string, fallback: number): number {
  const raw = process.env[name]
  if (!raw) return fallback
  const parsed = Number.parseInt(raw, 10)
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback
}

export function defaultRateLimiters(): FleetBusRateLimiters {
  const window = envInt('FLEET_BUS_RATE_WINDOW_MS', 60_000)
  return {
    perFrom: new FixedWindowBucket(envInt('FLEET_BUS_RATE_PER_FROM', 30), window),
    perSubject: new FixedWindowBucket(envInt('FLEET_BUS_RATE_PER_SUBJECT', 120), window),
    perSessionInject: new FixedWindowBucket(envInt('FLEET_BUS_RATE_PER_SESSION_INJECT', 30), window),
  }
}

/* -------------------------------------------------------------------------- */
/* Bounded LRU (Map insertion order + delete-on-touch)                         */
/* -------------------------------------------------------------------------- */

class BoundedLru<K, V> {
  private readonly store = new Map<K, V>()
  constructor(
    public readonly capacity: number,
    private readonly onEvict?: (key: K, value: V) => void,
  ) {}

  get size(): number {
    return this.store.size
  }

  has(key: K): boolean {
    return this.store.has(key)
  }

  get(key: K): V | undefined {
    const value = this.store.get(key)
    if (value === undefined) return undefined
    // Touch — re-insert to move to the end.
    this.store.delete(key)
    this.store.set(key, value)
    return value
  }

  set(key: K, value: V): void {
    if (this.store.has(key)) this.store.delete(key)
    this.store.set(key, value)
    while (this.store.size > this.capacity) {
      const oldest = this.store.keys().next().value
      if (oldest === undefined) break
      const evicted = this.store.get(oldest)!
      this.store.delete(oldest)
      this.onEvict?.(oldest, evicted)
    }
  }

  delete(key: K): boolean {
    return this.store.delete(key)
  }
}

/* -------------------------------------------------------------------------- */
/* Inflight ledger types                                                       */
/* -------------------------------------------------------------------------- */

interface InflightEntry {
  envelope: Envelope
  expectedFrom: string
  resolve: (result: FleetBusRequestResult) => void
  timerId: ReturnType<typeof setTimeout>
}

/* -------------------------------------------------------------------------- */
/* FleetBus                                                                    */
/* -------------------------------------------------------------------------- */

/**
 * Session-owned NATS transport. Stage 3 adds request/reply, supervisor loop,
 * suppression-aware reply inject, and per-key rate limiting.
 */
export class FleetBus {
  private nc?: NatsConnection
  private readonly subscriptions = new Set<Subscription>()
  private heartbeatTimer?: ReturnType<typeof setInterval>
  private readonly codec = JSONCodec<unknown>()
  private readonly mode: FleetBusMode
  private readonly outboundLedger: BoundedLru<string, InflightEntry>
  private readonly receiveLedger: BoundedLru<string, Envelope>
  private readonly evictedLedger: BoundedLru<string, true>
  private readonly rateLimiters: FleetBusRateLimiters
  private readonly connectFn: (options: ConnectionOptions) => Promise<NatsConnection>
  private supervisorStopping = false
  private closedResolver?: () => void

  constructor(
    private readonly config: FleetBusConfig,
    private readonly allowedFromClaims: ReadonlySet<string>,
  ) {
    this.mode = config.mode ?? 'primary'
    this.connectFn = config.connectFn ?? natsConnect
    this.rateLimiters = config.rateLimiters ?? defaultRateLimiters()
    this.outboundLedger = new BoundedLru<string, InflightEntry>(
      config.inflightLedgerCap ?? DEFAULT_INFLIGHT_LEDGER_CAP,
      (_key, entry) => this.onInflightEvict(entry),
    )
    this.receiveLedger = new BoundedLru<string, Envelope>(config.receiveLedgerCap ?? DEFAULT_RECEIVE_LEDGER_CAP)
    this.evictedLedger = new BoundedLru<string, true>(config.evictedLedgerCap ?? DEFAULT_EVICTED_LEDGER_CAP)
  }

  async connect(): Promise<void> {
    if (this.nc) return

    const botName = normalizeBotName(this.config.botName)
    const user = normalizeBotName(this.config.user)
    if (botName === null || user === null || botName !== user) {
      throw new Error('FleetBus botName and user must be the same canonical fleet identity')
    }

    const nc = await this.connectFn({
      servers: this.config.url,
      user,
      pass: this.config.password,
      inboxPrefix: `_INBOX_${botName}`,
      maxReconnectAttempts: -1,
    })

    try {
      this.nc = nc
      if (this.mode !== 'publish-only') {
        this.subscribe(`fleet.${botName}.request`, message => this.onRequest(message))
        this.subscribe(`fleet.${botName}.result`, message => this.onResult(message))
        this.subscribe(`fleet.${botName}.status`, message => this.onStatus(message))
        if (this.config.subscribeBroadcast) {
          this.subscribe('fleet.broadcast.>', message => this.onBroadcast(message))
        }
        this.publishHeartbeat()
        this.heartbeatTimer = setInterval(
          () => this.publishHeartbeat(),
          this.config.heartbeatIntervalMs ?? 30_000,
        )
      }
      this.log(`connected as ${botName}${this.mode === 'publish-only' ? ' [publish-only]' : ''}`)
      void this.watchConnectionStatus(nc)
    } catch (error) {
      this.nc = undefined
      await nc.close()
      throw error
    }
  }

  async disconnect(): Promise<void> {
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer)
    this.heartbeatTimer = undefined
    for (const subscription of this.subscriptions) subscription.unsubscribe()
    this.subscriptions.clear()

    const nc = this.nc
    this.nc = undefined
    if (nc && !nc.isClosed()) await nc.drain()
    const resolver = this.closedResolver
    this.closedResolver = undefined
    resolver?.()
  }

  /**
   * Run the supervisor loop: connect, wait for CLOSED, disconnect (clears
   * subscriptions/heartbeat/nc reference), sleep, retry. Closes only when
   * `stop()` is called.
   */
  async run(): Promise<void> {
    const sleepMs = this.config.supervisorSleepMs ?? 2_000
    this.supervisorStopping = false
    while (!this.supervisorStopping) {
      try {
        await this.connect()
      } catch (error) {
        this.log(`supervisor: connect failed: ${String(error)}`)
        await this.disconnect().catch(() => {})
        if (this.supervisorStopping) break
        await this.sleep(sleepMs)
        continue
      }
      await this.waitClosed()
      if (this.supervisorStopping) break
      this.log('supervisor: connection closed, reconnecting after backoff')
      await this.disconnect().catch(() => {})
      await this.sleep(sleepMs)
    }
    await this.disconnect().catch(() => {})
  }

  async stop(): Promise<void> {
    this.supervisorStopping = true
    const resolver = this.closedResolver
    this.closedResolver = undefined
    resolver?.()
    await this.disconnect()
  }

  private waitClosed(): Promise<void> {
    if (!this.nc) return Promise.resolve()
    const closedByServer = this.nc.closed().then(() => undefined).catch(() => undefined)
    const explicit = new Promise<void>(resolve => {
      this.closedResolver = resolve
    })
    return Promise.race([closedByServer, explicit])
  }

  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms))
  }

  async request(options: FleetBusRequestOptions): Promise<FleetBusRequestResult> {
    if (this.mode === 'publish-only' && options.wait === true) {
      return { ok: false, error: 'multi_instance_publish_only' }
    }
    if (!this.nc || this.nc.isClosed()) return { ok: false, error: 'fleet_bus_not_connected' }

    const canonicalTo = normalizeBotName(options.to)
    if (canonicalTo === null) return { ok: false, error: 'invalid_recipient' }
    const canonicalBot = normalizeBotName(this.config.botName)!
    const envelopeId = randomUUID()
    let baton: BatonFields
    try {
      baton = deriveBatonFields({
        envelopeId,
        botName: canonicalBot,
        inbound: options.inbound,
        kind: options.kind,
        recipient: options.to,
      })
    } catch (error) {
      const reason = error instanceof BatonDerivationError ? error.reason : 'baton_derivation_failed'
      this.recordAudit({ dir: 'drop', subject: `fleet.${canonicalTo}.request`, reason, envelope_id: envelopeId })
      return { ok: false, error: reason }
    }
    const envelope: Envelope = {
      envelope_version: 1,
      id: envelopeId,
      from: canonicalBot,
      to: canonicalTo,
      kind: options.kind,
      ts: new Date().toISOString(),
      payload: options.payload,
      ...baton,
    }
    const validation = validateEnvelope(envelope, this.allowedFromClaims, this.config.maxEnvelopeBytes ?? DEFAULT_MAX_ENVELOPE_BYTES)
    if (!validation.ok) {
      this.recordAudit({ dir: 'drop', subject: `fleet.${canonicalTo}.request`, reason: validation.error, envelope_id: envelopeId })
      return { ok: false, error: validation.error, envelope }
    }
    const subject = `fleet.${canonicalTo}.request`
    try {
      this.nc.publish(subject, this.codec.encode(envelope))
    } catch (error) {
      this.recordAudit({ dir: 'drop', subject, reason: 'publish_failed', envelope_id: envelopeId, error: String(error) })
      return { ok: false, error: 'publish_failed', envelope }
    }
    this.recordAudit({ dir: 'out', subject, envelope_id: envelopeId })

    if (options.wait !== true) {
      return { ok: true, envelope, delivered_to_subscriber: true }
    }

    const timeoutMs = options.timeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS
    return new Promise<FleetBusRequestResult>(resolve => {
      const timerId = setTimeout(() => {
        // Timeout fires — evict from ledger, remember for late-reply tagging, resolve as timed_out.
        if (this.outboundLedger.has(envelopeId)) {
          this.outboundLedger.delete(envelopeId)
          this.evictedLedger.set(envelopeId, true)
          this.recordAudit({ dir: 'drop', subject, reason: 'request_timeout', envelope_id: envelopeId })
          resolve({ ok: false, timed_out: true, envelope })
        }
      }, timeoutMs)
      this.outboundLedger.set(envelopeId, {
        envelope,
        expectedFrom: canonicalTo,
        resolve,
        timerId,
      })
    })
  }

  publishReply(reqId: string, payload: unknown, kind = 'result'): FleetBusReplyResult {
    if (this.mode === 'publish-only') {
      return { ok: false, error: 'multi_instance_publish_only', req_id: reqId }
    }
    if (!this.nc || this.nc.isClosed()) return { ok: false, error: 'fleet_bus_not_connected', req_id: reqId }
    const inbound = this.receiveLedger.get(reqId)
    if (inbound === undefined) return { ok: false, error: 'req_id_unknown', req_id: reqId }
    const canonicalBot = normalizeBotName(this.config.botName)!
    const envelopeId = randomUUID()
    let baton: BatonFields
    try {
      baton = deriveBatonFields({
        envelopeId,
        botName: canonicalBot,
        inbound,
        kind,
        recipient: inbound.from,
      })
    } catch (error) {
      const reason = error instanceof BatonDerivationError ? error.reason : 'baton_derivation_failed'
      this.recordAudit({ dir: 'drop', subject: `fleet.${inbound.from}.result`, reason, envelope_id: envelopeId })
      return { ok: false, error: reason, req_id: reqId }
    }
    const envelope: Envelope = {
      envelope_version: 1,
      id: envelopeId,
      from: canonicalBot,
      to: inbound.from,
      kind,
      // Wire correlation uses the inbound WIRE id, not the consumer-local reqId nonce (round-2 P1).
      in_reply_to: inbound.id,
      ts: new Date().toISOString(),
      payload,
      ...baton,
    }
    const validation = validateEnvelope(envelope, this.allowedFromClaims, this.config.maxEnvelopeBytes ?? DEFAULT_MAX_ENVELOPE_BYTES)
    if (!validation.ok) {
      this.recordAudit({ dir: 'drop', subject: `fleet.${inbound.from}.result`, reason: validation.error, envelope_id: envelopeId })
      return { ok: false, error: validation.error, req_id: reqId, envelope }
    }
    const subject = `fleet.${inbound.from}.result`
    try {
      this.nc.publish(subject, this.codec.encode(envelope))
    } catch (error) {
      this.recordAudit({ dir: 'drop', subject, reason: 'publish_failed', envelope_id: envelopeId, error: String(error) })
      return { ok: false, error: 'publish_failed', req_id: reqId, envelope }
    }
    this.recordAudit({ dir: 'out', subject, envelope_id: envelopeId, req_id: reqId })
    return { ok: true, envelope, req_id: reqId }
  }

  protected async onRequest(message: Msg): Promise<void> {
    const subject = message.subject
    if (!this.rateLimiters.perSubject.allow(subject)) {
      this.recordAudit({ dir: 'drop', subject, reason: 'claude_discord_adapter_rate_limited_per_subject' })
      return
    }
    let decoded: unknown
    try {
      decoded = this.codec.decode(message.data)
    } catch {
      this.recordAudit({ dir: 'drop', subject, reason: 'malformed_json' })
      return
    }
    const result = validateEnvelope(
      decoded,
      this.allowedFromClaims,
      this.config.maxEnvelopeBytes ?? DEFAULT_MAX_ENVELOPE_BYTES,
    )
    if (!result.ok) {
      this.recordAudit({ dir: 'drop', subject, reason: result.error })
      return
    }
    if (normalizeBotName(result.envelope.to) !== normalizeBotName(this.config.botName)) {
      this.recordAudit({
        dir: 'drop',
        subject,
        reason: 'recipient_mismatch',
        envelope_id: result.envelope.id,
      })
      return
    }
    if (!this.rateLimiters.perFrom.allow(result.envelope.from)) {
      this.recordAudit({
        dir: 'drop',
        subject,
        reason: 'claude_discord_adapter_rate_limited_per_from',
        envelope_id: result.envelope.id,
      })
      return
    }
    if (!this.rateLimiters.perSessionInject.allow('session')) {
      this.recordAudit({
        dir: 'drop',
        subject,
        reason: 'claude_discord_adapter_rate_limited_per_session_inject',
        envelope_id: result.envelope.id,
      })
      return
    }

    const reqId = randomBytes(16).toString('hex')
    this.receiveLedger.set(reqId, result.envelope)
    await this.injectIntoSession({ envelope: result.envelope, reqId }).catch(error => {
      this.recordAudit({ dir: 'drop', subject, reason: 'injection_failed', error: String(error) })
    })
  }

  protected async onResult(message: Msg): Promise<void> {
    const subject = message.subject
    if (!this.rateLimiters.perSubject.allow(subject)) {
      this.recordAudit({ dir: 'drop', subject, reason: 'claude_discord_adapter_rate_limited_per_subject' })
      return
    }
    let decoded: unknown
    try {
      decoded = this.codec.decode(message.data)
    } catch {
      this.recordAudit({ dir: 'drop', subject, reason: 'malformed_json' })
      return
    }
    const validation = validateEnvelope(
      decoded,
      this.allowedFromClaims,
      this.config.maxEnvelopeBytes ?? DEFAULT_MAX_ENVELOPE_BYTES,
    )
    if (!validation.ok) {
      this.recordAudit({ dir: 'drop', subject, reason: validation.error })
      return
    }
    const envelope = validation.envelope
    const canonicalBot = normalizeBotName(this.config.botName)
    const canonicalTo = envelope.to === null || envelope.to === undefined ? null : normalizeBotName(envelope.to)
    if (canonicalTo !== null && canonicalTo !== canonicalBot) {
      this.recordAudit({
        dir: 'drop',
        subject,
        reason: 'misrouted_reply',
        envelope_id: envelope.id,
      })
      return
    }
    if (!this.rateLimiters.perFrom.allow(envelope.from)) {
      this.recordAudit({
        dir: 'drop',
        subject,
        reason: 'claude_discord_adapter_rate_limited_per_from',
        envelope_id: envelope.id,
      })
      return
    }

    const inReplyTo = envelope.in_reply_to
    if (inReplyTo !== undefined) {
      const match = this.outboundLedger.get(inReplyTo)
      if (match !== undefined) {
        if (envelope.from === match.expectedFrom) {
          // Ledger-matched .result: resolve the outstanding waiter and BYPASS
          // perSessionInject — this resolves an already-running tool call
          // rather than initiating a new session turn (P2-5).
          clearTimeout(match.timerId)
          this.outboundLedger.delete(inReplyTo)
          this.recordAudit({ dir: 'in', subject, envelope_id: envelope.id, note: 'ledger_matched' })
          match.resolve({ ok: true, envelope: match.envelope, delivered_to_subscriber: true, reply: envelope })
          return
        }
        // From mismatch — anti-hijack (P2-1). Do NOT resolve the waiter; treat
        // as unsolicited and let the model see it if inject budget allows.
        this.recordAudit({
          dir: 'drop',
          subject,
          reason: 'reply_from_mismatch',
          envelope_id: envelope.id,
          expected_from: match.expectedFrom,
        })
        await this.injectUnsolicited(subject, envelope)
        return
      }
    }

    // No ledger match — inject as unsolicited. Tag as late-reply if we
    // remember evicting a request with this id (P2-2).
    const lateReplyEnvId = inReplyTo !== undefined && this.evictedLedger.has(inReplyTo) ? inReplyTo : undefined
    if (lateReplyEnvId !== undefined) this.evictedLedger.delete(lateReplyEnvId)
    await this.injectUnsolicited(subject, envelope, lateReplyEnvId)
  }

  private async injectUnsolicited(subject: string, envelope: Envelope, lateReplyEnvId?: string): Promise<void> {
    if (!this.rateLimiters.perSessionInject.allow('session')) {
      this.recordAudit({
        dir: 'drop',
        subject,
        reason: 'claude_discord_adapter_rate_limited_per_session_inject',
        envelope_id: envelope.id,
      })
      return
    }
    const reqId = randomBytes(16).toString('hex')
    this.receiveLedger.set(reqId, envelope)
    this.recordAudit({
      dir: 'in',
      subject,
      envelope_id: envelope.id,
      req_id: reqId,
      note: lateReplyEnvId !== undefined ? 'late_reply' : 'unsolicited_reply',
      ...(lateReplyEnvId !== undefined ? { late_reply_env_id: lateReplyEnvId } : {}),
    })
    await this.injectIntoSession({ envelope, reqId, unsolicited: true, lateReplyEnvId }).catch(error => {
      this.recordAudit({ dir: 'drop', subject, reason: 'injection_failed', error: String(error) })
    })
  }

  protected onStatus(message: Msg): void {
    this.log(`received ${message.subject}`)
  }

  protected onBroadcast(message: Msg): void {
    this.log(`received ${message.subject}`)
  }

  private onInflightEvict(entry: InflightEntry): void {
    // LRU eviction of a pending waiter — reject with ledger_overflow AND clear
    // the timer (not a silent limp-to-timeout, per round-3 P2-3).
    clearTimeout(entry.timerId)
    this.evictedLedger.set(entry.envelope.id, true)
    this.recordAudit({
      dir: 'drop',
      subject: `fleet.${entry.envelope.to}.request`,
      reason: 'ledger_overflow',
      envelope_id: entry.envelope.id,
    })
    entry.resolve({ ok: false, error: 'ledger_overflow', envelope: entry.envelope })
  }

  private subscribe(subject: string, handler: (message: Msg) => void | Promise<void>): void {
    if (!this.nc) throw new Error('FleetBus is not connected')
    const subscription = this.nc.subscribe(subject)
    this.subscriptions.add(subscription)
    void (async () => {
      try {
        for await (const message of subscription) await handler(message)
      } catch (error) {
        if (!this.nc?.isClosed()) this.log(`subscription ${subject} failed: ${String(error)}`)
      } finally {
        this.subscriptions.delete(subscription)
      }
    })()
  }

  private publishHeartbeat(): void {
    if (!this.nc || this.nc.isClosed()) return
    this.nc.publish(
      `fleet.${this.config.botName}.status`,
      this.codec.encode(createHeartbeatEnvelope(
        this.config.botName,
        this.config.pluginVersion ?? '0.4.0',
      )),
    )
  }

  private async watchConnectionStatus(nc: NatsConnection): Promise<void> {
    for await (const status of nc.status()) {
      if (status.type === 'disconnect' || status.type === 'reconnect' || status.type === 'error') {
        this.log(`${status.type}: ${String(status.data)}`)
      }
    }
  }

  private async injectIntoSession(event: FleetBusSessionEvent): Promise<void> {
    if (!this.config.injectIntoSession) {
      this.log(`received ${event.envelope.kind} from ${event.envelope.from}; session injection is not configured`)
      return
    }
    await this.config.injectIntoSession(event)
    this.recordAudit({ dir: 'in', envelope: event.envelope, req_id: event.reqId })
  }

  private recordAudit(entry: Record<string, unknown>): void {
    if (!this.config.auditLogPath) {
      this.log(JSON.stringify(entry))
      return
    }
    try {
      mkdirSync(dirname(this.config.auditLogPath), { recursive: true, mode: 0o700 })
      appendFileSync(this.config.auditLogPath, `${JSON.stringify({ ts: new Date().toISOString(), ...entry })}\n`, {
        encoding: 'utf8',
        mode: 0o600,
      })
      chmodSync(this.config.auditLogPath, 0o600)
    } catch (error) {
      this.log(`audit write failed: ${String(error)}`)
    }
  }

  private log(message: string): void {
    this.config.logger?.(`FleetBus: ${message}`)
  }
}

/**
 * Convenience: build a bus and start its supervisor loop.
 * Returns `{ bus, done }` — await `done` to block until `bus.stop()` is called,
 * or ignore it and call `bus.stop()` at shutdown.
 */
export function runSupervisor(
  config: FleetBusConfig,
  allowedFromClaims: ReadonlySet<string>,
): { bus: FleetBus; done: Promise<void> } {
  const bus = new FleetBus(config, allowedFromClaims)
  const done = bus.run()
  return { bus, done }
}
