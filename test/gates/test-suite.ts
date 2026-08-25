/**
 * fleet-bus GATE test suite. Manual invocation:
 *
 *   FLEET_BUS_LUNA_PASS=... FLEET_BUS_KAT_PASS=... FLEET_BUS_CONSOLE_PASS=... \
 *     bun test-suite.ts
 *
 * Each gate is a self-contained function that returns { name, pass, detail }.
 * The suite runs sequentially, prints a plain-text report, exits non-zero on
 * any failure. Designed to be runnable by cron/CI later — no interactive I/O.
 *
 * Assumes NATS is reachable at $FLEET_BUS_URL (default nats://127.0.0.1:4222).
 * Does NOT assume any specific plugin is connected — most gates spawn their
 * own subscribers on the bus for the duration of the test.
 */
import { connect, StringCodec, type NatsConnection } from 'nats'
import { randomUUID } from 'node:crypto'

const sc = StringCodec()
const URL = process.env.FLEET_BUS_URL || 'nats://127.0.0.1:4222'

function need(k: string): string {
  const v = process.env[k]
  if (!v) throw new Error(`${k} required`)
  return v
}
const LUNA_PASS = need('FLEET_BUS_LUNA_PASS')
const KAT_PASS = need('FLEET_BUS_KAT_PASS')
const CONSOLE_PASS = need('FLEET_BUS_CONSOLE_PASS')

async function open(user: string, pass: string): Promise<NatsConnection> {
  return connect({
    servers: URL,
    user,
    pass,
    name: `test-${user}-${randomUUID().slice(0, 8)}`,
    inboxPrefix: `_INBOX_${user}`,
  })
}

interface Envelope {
  envelope_version: number
  id: string
  from: string
  to: string | null
  kind: string
  in_reply_to?: string
  ts: string
  payload: unknown
}
function envelope(from: string, to: string | null, kind: string, payload: unknown, in_reply_to?: string): Envelope {
  return {
    envelope_version: 1,
    id: randomUUID(),
    from,
    to,
    kind,
    ...(in_reply_to ? { in_reply_to } : {}),
    ts: new Date().toISOString(),
    payload,
  }
}
const publishEnv = (nc: NatsConnection, subject: string, env: Envelope) =>
  nc.publish(subject, sc.encode(JSON.stringify(env)))

interface Result { name: string; pass: boolean; detail: string }

async function withTimeout<T>(p: Promise<T>, ms: number, label: string): Promise<T> {
  return Promise.race([
    p,
    new Promise<T>((_, rej) => setTimeout(() => rej(new Error(`${label} timeout after ${ms}ms`)), ms)),
  ])
}

/* ---------- G1: host↔host round-trip via Luna & Kat identities ---------- */
async function g1(): Promise<Result> {
  const luna = await open('luna', LUNA_PASS)
  const kat = await open('kat', KAT_PASS)
  try {
    const probeId = randomUUID()
    const got = new Promise<Envelope>((resolve) => {
      const sub = kat.subscribe('fleet.kat.request')
      ;(async () => {
        for await (const msg of sub) {
          const env = JSON.parse(sc.decode(msg.data)) as Envelope
          if (env.id === probeId) {
            resolve(env)
            sub.unsubscribe()
            return
          }
        }
      })()
    })
    publishEnv(luna, 'fleet.kat.request', {
      ...envelope('luna', 'kat', 'text_message', { text: 'g1 probe' }),
      id: probeId,
    })
    const received = await withTimeout(got, 2000, 'g1 receive')
    return {
      name: 'G1 host↔host round-trip',
      pass: received.from === 'luna' && received.id === probeId,
      detail: `received id=${received.id.slice(0, 8)} from=${received.from}`,
    }
  } finally {
    await luna.drain()
    await kat.drain()
  }
}

/* ---------- G3: request/reply timeout when no responder ---------- */
async function g3(): Promise<Result> {
  const luna = await open('luna', LUNA_PASS)
  try {
    const start = Date.now()
    try {
      await luna.request('fleet.deet.request', sc.encode(JSON.stringify(envelope('luna', 'deet', 'text_message', { text: 'expect timeout' }))), { timeout: 500 })
      return { name: 'G3 request/reply timeout', pass: false, detail: 'request returned instead of timing out' }
    } catch (err) {
      const elapsed = Date.now() - start
      const msg = err instanceof Error ? err.message : String(err)
      const isTimeout = /timeout|no.*responder/i.test(msg)
      return {
        name: 'G3 request/reply timeout',
        pass: isTimeout && elapsed >= 400 && elapsed <= 1500,
        detail: `err=${msg} elapsed=${elapsed}ms`,
      }
    }
  } finally {
    await luna.drain()
  }
}

/* ---------- G4: permission enforcement (console publish is denied end-to-end) ---------- */
async function g4(): Promise<Result> {
  const consoleNc = await open('console', CONSOLE_PASS)
  const kat = await open('kat', KAT_PASS)
  try {
    const probeId = randomUUID()
    let landed = false
    let permErr = ''
    consoleNc.closed().then((err) => { if (err) permErr = String(err) })
    const sub = kat.subscribe('fleet.kat.request')
    ;(async () => {
      for await (const m of sub) {
        const env = JSON.parse(sc.decode(m.data)) as Envelope
        if (env.id === probeId) { landed = true; sub.unsubscribe(); return }
      }
    })()
    await new Promise((r) => setTimeout(r, 100))
    try {
      publishEnv(consoleNc, 'fleet.kat.request', { ...envelope('console', 'kat', 'text_message', { text: 'should be denied' }), id: probeId })
      await consoleNc.flush()
    } catch {}
    await new Promise((r) => setTimeout(r, 500))
    sub.unsubscribe()
    return {
      name: 'G4 permission enforcement (console → publish deny)',
      pass: !landed,
      detail: landed
        ? `SECURITY: console-published envelope ${probeId.slice(0, 8)} was DELIVERED to fleet.kat.request. deny-all publish is not enforced.`
        : `blocked as expected (perm_err=${permErr || 'none'})`,
    }
  } finally {
    try { await consoleNc.drain() } catch {}
    try { await kat.drain() } catch {}
  }
}

/* ---------- G5: broadcast delivery (2 subscribers, 1 publish) ---------- */
async function g5(): Promise<Result> {
  const luna = await open('luna', LUNA_PASS)
  const kat = await open('kat', KAT_PASS)
  try {
    const bcastId = randomUUID()
    let receivedByLuna = false
    let receivedByKat = false
    const lunaSub = luna.subscribe('fleet.broadcast.>')
    const katSub = kat.subscribe('fleet.broadcast.>')
    ;(async () => {
      for await (const m of lunaSub) {
        const env = JSON.parse(sc.decode(m.data)) as Envelope
        if (env.id === bcastId) { receivedByLuna = true; lunaSub.unsubscribe(); return }
      }
    })()
    ;(async () => {
      for await (const m of katSub) {
        const env = JSON.parse(sc.decode(m.data)) as Envelope
        if (env.id === bcastId) { receivedByKat = true; katSub.unsubscribe(); return }
      }
    })()
    await new Promise((r) => setTimeout(r, 100))
    publishEnv(luna, 'fleet.broadcast.test', { ...envelope('luna', null, 'test_broadcast', { hello: 'world' }), id: bcastId })
    await new Promise((r) => setTimeout(r, 500))
    return {
      name: 'G5 broadcast delivery',
      pass: receivedByLuna && receivedByKat,
      detail: `luna_got=${receivedByLuna} kat_got=${receivedByKat}`,
    }
  } finally {
    await luna.drain()
    await kat.drain()
  }
}

/* ---------- G6: large payload (10 KB, round-trip intact) ---------- */
async function g6(): Promise<Result> {
  const luna = await open('luna', LUNA_PASS)
  const kat = await open('kat', KAT_PASS)
  try {
    const big = 'x'.repeat(10 * 1024)
    const probeId = randomUUID()
    const got = new Promise<Envelope>((resolve) => {
      const sub = kat.subscribe('fleet.kat.request')
      ;(async () => {
        for await (const m of sub) {
          const env = JSON.parse(sc.decode(m.data)) as Envelope
          if (env.id === probeId) { resolve(env); sub.unsubscribe(); return }
        }
      })()
    })
    publishEnv(luna, 'fleet.kat.request', { ...envelope('luna', 'kat', 'text_message', { text: big }), id: probeId })
    const rx = await withTimeout(got, 2000, 'g6 receive')
    const payloadText = (rx.payload as { text: string }).text
    return {
      name: 'G6 large payload (10 KB)',
      pass: payloadText === big,
      detail: `sent=${big.length}B received=${payloadText.length}B match=${payloadText === big}`,
    }
  } finally {
    await luna.drain()
    await kat.drain()
  }
}

/* ---------- G7: offline delivery — verify Core NATS is LOSSY by design ---------- */
async function g7(): Promise<Result> {
  // Use a subject only luna has perms on (_INBOX_luna.>), so we can both
  // publish and subscribe with luna's identity while proving no message
  // survives the gap between publish and subscribe.
  const testSubject = `_INBOX_luna.g7-${randomUUID().slice(0, 8)}`
  const publisher = await open('luna', LUNA_PASS)
  try {
    publisher.publish(testSubject, sc.encode('lost-if-no-subscriber'))
    await publisher.flush()
    await new Promise((r) => setTimeout(r, 100))
  } finally {
    await publisher.drain()
  }
  const subscriber = await open('luna', LUNA_PASS)
  try {
    let anythingArrived = false
    const sub = subscriber.subscribe(testSubject)
    ;(async () => {
      for await (const _ of sub) { anythingArrived = true; sub.unsubscribe(); return }
    })()
    await new Promise((r) => setTimeout(r, 500))
    sub.unsubscribe()
    return {
      name: 'G7 offline delivery (Core NATS lossy contract)',
      pass: !anythingArrived,
      detail: anythingArrived
        ? 'received a message that was published before subscription — Core NATS is NOT lossy as expected'
        : 'as-designed: message dropped since no subscriber existed at publish time',
    }
  } finally {
    await subscriber.drain()
  }
}

/* ---------- runner ---------- */
async function main() {
  const gates = [g1, g3, g4, g5, g6, g7]
  const results: Result[] = []
  for (const g of gates) {
    try {
      results.push(await g())
    } catch (err) {
      results.push({ name: g.name, pass: false, detail: `THREW: ${err instanceof Error ? err.message : String(err)}` })
    }
  }

  const pad = Math.max(...results.map((r) => r.name.length))
  console.log('\nfleet-bus GATE test suite — ' + new Date().toISOString())
  console.log('-'.repeat(pad + 60))
  for (const r of results) {
    const mark = r.pass ? '✅' : '❌'
    console.log(`${mark}  ${r.name.padEnd(pad)}  ${r.detail}`)
  }
  const passed = results.filter((r) => r.pass).length
  console.log('-'.repeat(pad + 60))
  console.log(`${passed}/${results.length} passed`)

  process.exit(passed === results.length ? 0 : 1)
}

main()
