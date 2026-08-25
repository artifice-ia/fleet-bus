import { connect as natsConnect, JSONCodec, type NatsConnection, type Msg, type Subscription } from 'nats'
import { randomBytes, randomUUID } from 'node:crypto'
import { appendFileSync, chmodSync, mkdirSync, readFileSync } from 'node:fs'
import { dirname } from 'node:path'
import { parse as parseYaml } from 'yaml'

export const DEFAULT_MAX_ENVELOPE_BYTES = 1_044_480

export interface Envelope<P = unknown> {
  envelope_version: 1
  id: string
  from: string
  to?: string | null
  kind: string
  in_reply_to?: string
  ts: string
  payload: P
}

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
}

export interface FleetBusSessionEvent {
  envelope: Envelope
  reqId: string
}

export interface FleetBusFrameMeta {
  source: 'fleet-bus'
  authenticated: 'false'
  from_claim: string
  kind: string
  req_id: string
  env_id: string
  ts: string
}

export function buildFleetBusFrameMeta({ envelope, reqId }: FleetBusSessionEvent): FleetBusFrameMeta {
  return {
    source: 'fleet-bus',
    authenticated: 'false',
    from_claim: envelope.from,
    kind: envelope.kind,
    req_id: reqId,
    env_id: envelope.id,
    ts: envelope.ts,
  }
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
}

export interface FleetBusRequestResult {
  ok: boolean
  envelope?: Envelope
  delivered_to_subscriber?: boolean
  error?: string
}

export interface FleetBusReplyResult {
  ok: boolean
  envelope?: Envelope
  error?: 'req_id_unknown' | string
  req_id?: string
}

export type EnvelopeValidationResult =
  | { ok: true; envelope: Envelope }
  | { ok: false; error: string }

const BOT_NAME_PATTERN = /^[a-z0-9_-]+$/

/** Return the canonical bus identity, or null for a non-ASCII/invalid claim. */
export function normalizeBotName(value: unknown): string | null {
  if (typeof value !== 'string') return null
  const normalized = value.normalize('NFKC').toLowerCase()
  return BOT_NAME_PATTERN.test(normalized) ? normalized : null
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

/** Validate the v1 wire envelope before it reaches any bus handler. */
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

  let encodedBytes: number
  try {
    encodedBytes = Buffer.byteLength(JSON.stringify(candidate), 'utf8')
  } catch {
    return { ok: false, error: 'payload_not_serializable' }
  }
  if (encodedBytes > maxBytes) return { ok: false, error: 'envelope_too_large' }

  return { ok: true, envelope: { ...candidate, from } as unknown as Envelope }
}

/**
 * Session-owned NATS transport. Stage 1 defines its contract only; transport,
 * subscriptions, heartbeat, injection, ledgers, and rate limiting land in the
 * subsequent implementation commits described by the v0.6 spec.
 */
export class FleetBus {
  private nc?: NatsConnection
  private readonly subscriptions = new Set<Subscription>()
  private heartbeatTimer?: ReturnType<typeof setInterval>
  private readonly codec = JSONCodec<unknown>()

  constructor(
    private readonly config: FleetBusConfig,
    private readonly allowedFromClaims: ReadonlySet<string>,
  ) {}

  async connect(): Promise<void> {
    if (this.nc) return

    const botName = normalizeBotName(this.config.botName)
    const user = normalizeBotName(this.config.user)
    if (botName === null || user === null || botName !== user) {
      throw new Error('FleetBus botName and user must be the same canonical fleet identity')
    }

    const nc = await natsConnect({
      servers: this.config.url,
      user,
      pass: this.config.password,
      inboxPrefix: `_INBOX_${botName}`,
    })

    try {
      this.nc = nc
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
      this.log(`connected as ${botName}`)
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
  }

  async request(_options: FleetBusRequestOptions): Promise<FleetBusRequestResult> {
    // TODO(stage-3): publish request and optionally await the ephemeral inbox.
    throw new Error('FleetBus.request is not implemented')
  }

  publishReply(_reqId: string, _payload: unknown, _kind = 'result'): FleetBusReplyResult {
    // TODO(stage-3): resolve the server-side inflight ledger and publish reply.
    throw new Error('FleetBus.publishReply is not implemented')
  }

  protected async onRequest(message: Msg): Promise<void> {
    let decoded: unknown
    try {
      decoded = this.codec.decode(message.data)
    } catch {
      this.recordAudit({ dir: 'drop', subject: message.subject, reason: 'malformed_json' })
      return
    }
    const result = validateEnvelope(
      decoded,
      this.allowedFromClaims,
      this.config.maxEnvelopeBytes ?? DEFAULT_MAX_ENVELOPE_BYTES,
    )
    if (!result.ok) {
      this.recordAudit({ dir: 'drop', subject: message.subject, reason: result.error })
      return
    }
    if (normalizeBotName(result.envelope.to) !== normalizeBotName(this.config.botName)) {
      this.recordAudit({
        dir: 'drop',
        subject: message.subject,
        reason: 'recipient_mismatch',
        envelope_id: result.envelope.id,
      })
      return
    }

    const reqId = randomBytes(16).toString('hex')
    await this.injectIntoSession(result.envelope, reqId).catch(error => {
      this.recordAudit({ dir: 'drop', subject: message.subject, reason: 'injection_failed', error: String(error) })
    })
  }

  protected onResult(message: Msg): void {
    this.log(`received ${message.subject}`)
    // TODO(stage-3): validate, gate, dedupe, and resolve waiter or inject result.
  }

  protected onStatus(message: Msg): void {
    this.log(`received ${message.subject}`)
  }

  protected onBroadcast(message: Msg): void {
    this.log(`received ${message.subject}`)
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

  private async injectIntoSession(envelope: Envelope, reqId: string): Promise<void> {
    if (!this.config.injectIntoSession) {
      this.log(`received ${envelope.kind} from ${envelope.from}; session injection is not configured`)
      return
    }
    await this.config.injectIntoSession({ envelope, reqId })
    this.recordAudit({ dir: 'in', envelope, req_id: reqId })
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
