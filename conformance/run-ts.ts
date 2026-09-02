/**
 * TypeScript conformance runner for conformance/envelope.v1.vectors.json.
 *
 * Runs the normative vector set against this repo's `validateEnvelope`.
 * See conformance/README.md for the contract this implements.
 *
 * Usage: bun conformance/run-ts.ts
 */
import { readFileSync } from 'node:fs'
import { validateEnvelope, normalizeAllowlist } from '../src/fleet-bus.ts'

const IMPL = 'typescript'
/** This implementation's audit prefix on baton-field reasons. Stripped once, never substring-matched. */
const REASON_PREFIX = 'claude_discord_adapter_'

interface Vector {
  name: string
  why: string
  envelope?: Record<string, unknown>
  envelope_raw?: unknown
  envelope_generator?: string
  expect: 'accept' | 'reject'
  reason?: string
  normalized_from?: string
  allowed_from_override?: string[]
  status?: string
  actual?: Record<string, 'accept' | 'reject'>
}

const suite = JSON.parse(readFileSync(new URL('./envelope.v1.vectors.json', import.meta.url), 'utf8'))
const allowed = normalizeAllowlist(suite.allowed_from as string[])
const maxBytes: number = suite.max_bytes

/** Build an envelope whose compact UTF-8 encoding is exactly `target` bytes. */
function padTo(target: number): Record<string, unknown> {
  const base = {
    envelope_version: 1,
    id: 'size',
    from: 'deet',
    kind: 'text_message',
    ts: '2026-09-02T00:00:00Z',
    payload: { pad: '' },
  }
  const overhead = Buffer.byteLength(JSON.stringify(base), 'utf8')
  const padLength = target - overhead
  if (padLength < 0) throw new Error(`target ${target} is below the ${overhead}-byte envelope floor`)
  const built = { ...base, payload: { pad: 'a'.repeat(padLength) } }
  const actual = Buffer.byteLength(JSON.stringify(built), 'utf8')
  // The generator is only useful if it lands on the boundary exactly; approximating
  // here would silently turn the boundary vectors into ordinary size vectors.
  if (actual !== target) throw new Error(`generator missed: wanted ${target} bytes, built ${actual}`)
  return built
}

const GENERATORS: Record<string, () => unknown> = {
  pad_to_exact_bytes: () => padTo(maxBytes),
  pad_to_one_over_bytes: () => padTo(maxBytes + 1),
}

function inputFor(vector: Vector): unknown {
  if (vector.envelope_generator) {
    const generator = GENERATORS[vector.envelope_generator]
    if (!generator) throw new Error(`unknown generator: ${vector.envelope_generator}`)
    return generator()
  }
  if (vector.envelope !== undefined) return vector.envelope
  if ('envelope_raw' in vector) return vector.envelope_raw
  throw new Error(`vector ${vector.name} has no input`)
}

/** Strip at most one leading adapter prefix. Never a substring or suffix match. */
function unprefix(reason: string): string {
  return reason.startsWith(REASON_PREFIX) ? reason.slice(REASON_PREFIX.length) : reason
}

const failures: string[] = []
let normative = 0
let divergent = 0

for (const vector of suite.vectors as Vector[]) {
  const isDivergence = vector.status === 'known_divergence'
  // Divergences assert what this implementation does TODAY, so the runner also
  // fails when a divergence is silently fixed — that forces the vector file to
  // be updated in the same change as the fix.
  const expected = isDivergence ? vector.actual?.[IMPL] : vector.expect
  if (expected === undefined) {
    failures.push(`${vector.name}: divergence records no '${IMPL}' behaviour`)
    continue
  }
  isDivergence ? divergent++ : normative++

  let result: ReturnType<typeof validateEnvelope>
  try {
    // Built directly, NOT through normalizeAllowlist: an override exists to hold a
    // name the normalizer refuses, and running it through the very code under test
    // would hand the vector back its own answer.
    const vectorAllowed = vector.allowed_from_override
      ? new Set(vector.allowed_from_override)
      : allowed
    result = validateEnvelope(inputFor(vector), vectorAllowed, maxBytes)
  } catch (error) {
    failures.push(`${vector.name}: threw ${String(error)}`)
    continue
  }

  const got = result.ok ? 'accept' : 'reject'
  if (got !== expected) {
    const note = isDivergence ? ` [divergence: recorded ${IMPL}=${expected}]` : ''
    failures.push(`${vector.name}: expected ${expected}, got ${got}${note}` +
      (result.ok ? '' : ` (${result.error})`))
    continue
  }

  // A divergence whose correct behaviour differs from today's is asserted only
  // on the accept/reject axis; its reason is whatever this implementation says.
  if (got === 'reject' && vector.reason && !(isDivergence && vector.expect !== expected)) {
    const actualReason = unprefix((result as { error: string }).error)
    if (actualReason !== vector.reason) {
      failures.push(`${vector.name}: expected reason '${vector.reason}', got '${actualReason}'`)
      continue
    }
  }

  if (got === 'accept' && vector.normalized_from) {
    const from = (result as { envelope: { from: string } }).envelope.from
    if (from !== vector.normalized_from) {
      failures.push(`${vector.name}: expected normalized from '${vector.normalized_from}', got '${from}'`)
    }
  }
}

console.log(`${IMPL}: ${normative} normative + ${divergent} divergence vectors`)
if (failures.length > 0) {
  console.error(`\n${failures.length} conformance failure(s):`)
  for (const failure of failures) console.error(`  - ${failure}`)
  process.exit(1)
}
console.log('all vectors pass')
