// Tiny benchmark of the babeldb Node.js binding: puts/s and gets/s with
// 512-byte values, for 1 caller and for N concurrent promises, in both
// durability modes. Run: node bench.mjs   (BENCH_MS=ms per phase, default 2000)
import { createRequire } from 'node:module'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { randomBytes } from 'node:crypto'

const require = createRequire(import.meta.url)
const { open } = require('./index.js')

const PHASE_MS = Number(process.env.BENCH_MS ?? 2000)
const CONCURRENCY = [1, 8, 64]
const value = randomBytes(512)
const key = (i) => `bench/${String(i).padStart(10, '0')}`

/** `concurrency` async loops calling `op(i)` for PHASE_MS; returns ops/s. */
async function measure(concurrency, op) {
  let next = 0
  let done = 0
  const start = performance.now()
  const deadline = start + PHASE_MS
  const loop = async () => {
    while (performance.now() < deadline) {
      await op(next++)
      done++
    }
  }
  await Promise.all(Array.from({ length: concurrency }, loop))
  return done / ((performance.now() - start) / 1000)
}

/** A synchronous loop calling `op(i)` for PHASE_MS; returns ops/s. */
function measureSync(op) {
  let done = 0
  const start = performance.now()
  const deadline = start + PHASE_MS
  while (performance.now() < deadline) op(done++)
  return done / ((performance.now() - start) / 1000)
}

const fmt = (n) => Math.round(n).toLocaleString('en-US').padStart(9)
const root = mkdtempSync(join(tmpdir(), 'babeldb-node-bench-'))
console.log(`babeldb node bench: 512 B values, ${PHASE_MS} ms per phase, Node ${process.version}, ${process.platform}-${process.arch}`)
try {
  for (const durability of ['immediate', 'buffered']) {
    const db = open(join(root, durability), { durability })
    let written = 0
    for (const c of CONCURRENCY) {
      const base = written
      const rate = await measure(c, (i) => db.put(key(base + i), value))
      written += Math.round((rate * PHASE_MS) / 1000) + c
      console.log(`${durability.padEnd(9)} put  ${String(c).padStart(2)} concurrent: ${fmt(rate)} ops/s`)
    }
    await db.sync()
    // Only keys certainly written: the count above is an estimate.
    const keys = (await db.keys({ prefix: 'bench/' })).length
    for (const c of CONCURRENCY) {
      const rate = await measure(c, (i) => db.get(key(i % keys)))
      console.log(`${durability.padEnd(9)} get  ${String(c).padStart(2)} concurrent: ${fmt(rate)} ops/s`)
    }
    const putSync = measureSync((i) => db.putSync(key(i), value))
    console.log(`${durability.padEnd(9)} putSync  1 caller   : ${fmt(putSync)} ops/s`)
    const getSync = measureSync((i) => db.getSync(key(i % keys)))
    console.log(`${durability.padEnd(9)} getSync  1 caller   : ${fmt(getSync)} ops/s`)
    db.close()
  }
} finally {
  rmSync(root, { recursive: true, force: true })
}
