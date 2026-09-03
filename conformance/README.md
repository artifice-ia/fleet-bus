# Envelope v1 conformance vectors

`envelope.v1.vectors.json` is the normative cross-implementation test set for the
fleet-bus v1 envelope. Every implementation of `validateEnvelope` runs it in its
own CI, in its own language, against its own validator. It exists because the
schema constrains *shape* and the SPEC constrains *behaviour*, and neither one
catches two validators that read the same sentence differently.

Today there are two implementations:

| Implementation | Repo | Entry point |
|---|---|---|
| TypeScript | `artifice-ia/fleet-bus` | `validateEnvelope` in `src/fleet-bus.ts` |
| Python | `artifice-ia/codex-container` | `validate_envelope` in `bus.py` |

## What a runner must do

1. Load the file and build the allowlist from `allowed_from`, the ceiling from
   `max_bytes`.
2. For each vector, produce the input:
   - `envelope` — pass as-is.
   - `envelope_raw` — pass as-is; this is deliberately not an object.
   - `envelope_generator` — construct it (see **Generators**).
3. Call the implementation's validator.
4. Compare against `expect`, and for a rejection also against `reason`.
5. If `normalized_from` is present, assert the accepted envelope carries that
   canonical value — an accept that keeps the raw claim is a failure.

If a vector carries `allowed_from_override`, use it in place of the suite-level
allowlist for that vector — and **build the set directly, not through the
implementation's manifest normalizer.** An override exists precisely to hold a
name the normalizer refuses, so normalizing it would hand the vector back its
own answer and silently restore the hole the override was added to close.

A vector may also carry `mutation_witness`. It is **non-normative free text and
every runner ignores it** — a review recipe, naming the source change that was
observed to kill this vector and nothing else. Re-run it by hand when the code
it names is touched.

It is deliberately not proof. Nothing checks that the recipe still applies, that
the symbol it names still exists, or that the mutation still fails exactly one
vector, so a stale recipe leaves CI green. Read it as a pointer to where a
reviewer should aim, and re-derive the result rather than citing the field.

## Reason codes

Vectors name reasons in their **unprefixed** form: `invalid_root_id`, not
`claude_discord_adapter_invalid_root_id` or `codex_adapter_invalid_root_id`.
Each implementation emits its own adapter prefix on the baton-field reasons,
and that prefix is genuinely useful in an audit log — it says which side refused.

So a runner declares its own prefix and strips at most one leading occurrence of
it before comparing. A runner MUST NOT do a substring or suffix match: that
would let `invalid_hops` satisfy a vector expecting `invalid_hops_ceiling`, and
those are two different refusals with two different causes.

## Divergences

A vector with `"status": "known_divergence"` is one the implementations disagree
on today. `expect`/`reason` state the behaviour we have decided is **correct**;
`actual` records what each implementation does **right now**, and `resolution`
says how it gets closed.

Runners assert `actual`, not `expect` — so CI is green while a divergence stands.
That is deliberate, and so is the other half: an implementation that *stops*
matching its recorded `actual` also fails, including when it starts doing the
correct thing. Fixing a divergence is therefore required to update this file in
the same change, which is the only mechanism that keeps `actual` honest. A
divergence that could be quietly fixed and left recorded is a divergence that
will be re-introduced later by someone reading this file as current.

Do not add a divergence to make a red runner green. A divergence is a decision
that one implementation is wrong, written down with the fix; it is not a way to
record that the two differ and move on.

## Generators

Two vectors are generated rather than literal, because the size boundary has to
be exact and a hand-written 1 MiB literal would not be:

| Generator | Builds |
|---|---|
| `pad_to_exact_bytes` | A valid envelope whose compact encoding is exactly `max_bytes` |
| `pad_to_one_over_bytes` | The same, at `max_bytes + 1` |

Both pad a single ASCII string field, so one character is one byte and the
target length is reachable exactly. Encode compactly — no spaces after `:` or
`,` — and measure UTF-8 bytes, not characters. A runner that cannot hit the
target exactly MUST fail rather than approximate: an off-by-one here is the
whole point of the vector.

## Scope

These vectors cover **validation of a decoded wire frame**. Deliberately outside:

- **Send-side payload serializability.** `NaN`, `undefined`, functions and
  BigInt cannot be written into a JSON vector file, so they cannot be tested
  here; they are unit-test material in each implementation. Note that the two
  differ: `bus.py` encodes with `allow_nan=False` and refuses, while
  `validateEnvelope` encodes with a bare `JSON.stringify` and silently coerces
  `NaN` to `null` on the wire — `payloadIsJsonSerializable` exists in the same
  file for exactly this and is not called by the validator. Tracked as INF-039.
- **Recipient matching.** `bus.py` takes a `recipient` argument and can return
  `recipient_mismatch`; the TypeScript validator has no such parameter and does
  that check at the subscription. It is a receiver-side concern, not an envelope
  property, so it is not a divergence and gets no vector.
- **Transport.** Subjects, JetStream consumers, dedup ledgers, rate limits.

## Changing this file

Appending a vector is additive. **Altering or removing one is a breaking change
to the contract between two repositories** and needs a SPEC.md revision, because
the other implementation's CI is pinned to a version of this file and will fail
on the change it did not ask for. That failure is working as intended.
