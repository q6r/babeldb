// Tests of the babeldb Node.js binding (node:test). Run: npm test
import { test, after } from 'node:test'
import assert from 'node:assert/strict'
import { createRequire } from 'node:module'
import { mkdtempSync, rmSync, realpathSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, isAbsolute, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { spawnSync } from 'node:child_process'
import { randomBytes } from 'node:crypto'

const require = createRequire(import.meta.url)
const indexPath = join(dirname(fileURLToPath(import.meta.url)), '..', 'index.js')
const { open, messageKey, parseMessageKey, channelPrefix, Database } = require(indexPath)

const root = mkdtempSync(join(tmpdir(), 'babeldb-node-test-'))
let dirs = 0
const newDir = () => join(root, `db${dirs++}`)
after(() => rmSync(root, { recursive: true, force: true }))

/** assert.throws / assert.rejects validator: the error has this `code`. */
const code = (expected) => (err) => {
  assert.equal(err.code, expected, `expected code ${expected}, got ${err.code}: ${err.message}`)
  assert.ok(err instanceof Error)
  return true
}
const strings = (buffers) => buffers.map((b) => b.toString())
const scanKeys = (entries) => entries.map(([k]) => k.toString())

/** Run `script` in a child Node.js process (the addon loaded there). */
function child(script, args = []) {
  const r = spawnSync(process.execPath, [...args, '-e', script], { encoding: 'utf8' })
  assert.equal(r.status, 0, `child failed (${r.status}): ${r.stderr}`)
  return r.stdout
}

test('roundtrip with strings, Buffers and Uint8Arrays', async () => {
  const db = open(newDir())
  const r1 = await db.put('hello', 'world')
  assert.equal(typeof r1, 'bigint')
  assert.ok(r1 > 0n)
  const got = await db.get('hello')
  assert.ok(Buffer.isBuffer(got))
  assert.deepEqual(got, Buffer.from('world'))
  assert.deepEqual(await db.get(Buffer.from('hello')), Buffer.from('world'), 'a string key is its UTF-8 bytes')

  const key = Buffer.from([0, 1, 2, 0xff])
  await db.put(key, new Uint8Array([9, 8, 7]))
  assert.deepEqual([...(await db.get(key))], [9, 8, 7])
  const view = new Uint8Array([100, 101, 102, 103, 104]).subarray(1, 4)
  await db.put(view, view)
  assert.deepEqual([...(await db.get(Buffer.from([101, 102, 103])))], [101, 102, 103])

  await db.put('ключ', 'значение 🚀')
  assert.equal((await db.get('ключ')).toString('utf8'), 'значение 🚀')
  await db.put('empty', '')
  assert.deepEqual(await db.get('empty'), Buffer.alloc(0))
  assert.equal(await db.get('missing'), null)

  const r2 = db.putSync('sync', 'value')
  assert.ok(r2 > r1)
  assert.deepEqual(db.getSync('sync'), Buffer.from('value'))
  assert.equal(db.getSync('nope'), null)

  await assert.rejects(db.put(42, 'x'), code('INVALID_ARGUMENT'))
  await assert.rejects(db.get({}), code('INVALID_ARGUMENT'))
  await assert.rejects(db.put('k', new Uint16Array(2)), code('INVALID_ARGUMENT'))
  await assert.rejects(db.put('', 'empty key'), code('INVALID_ARGUMENT'))
  await assert.rejects(db.put('k'.repeat(5000), 'key over 4096 bytes'), code('INVALID_ARGUMENT'))
  assert.throws(() => db.putSync('k', 12), code('INVALID_ARGUMENT'))
  assert.throws(() => db.getSync(), code('INVALID_ARGUMENT'))
  db.close()
})

test('1 MiB values', async () => {
  const db = open(newDir())
  const random = randomBytes(1 << 20)
  const text = Buffer.alloc(1 << 20, 'abc')
  await db.put('big/random', random)
  await db.put('big/text', text)
  assert.ok((await db.get('big/random')).equals(random))
  assert.ok((await db.get('big/text')).equals(text))
  db.putSync('big/sync', random)
  assert.ok(db.getSync('big/sync').equals(random))
  const entries = await db.scan({ prefix: 'big/' })
  assert.deepEqual(scanKeys(entries), ['big/random', 'big/sync', 'big/text'])
  assert.ok(entries[0][1].equals(random))
  assert.ok(entries[2][1].equals(text))
  db.close()
})

test('compare-and-set with ifAbsent / ifRevision', async () => {
  const db = open(newDir())
  const r1 = await db.put('k', 'v1', { ifAbsent: true })
  await assert.rejects(db.put('k', 'v2', { ifAbsent: true }), code('CONFLICT'))
  const r2 = await db.put('k', 'v2', { ifRevision: r1 })
  assert.ok(r2 > r1)
  await assert.rejects(db.put('k', 'stale', { ifRevision: r1 }), code('CONFLICT'))
  assert.deepEqual(await db.get('k'), Buffer.from('v2'))
  const r3 = await db.put('k', 'v3', { ifRevision: Number(r2) }) // a number works too
  assert.ok(r3 > r2)
  await assert.rejects(db.put('absent', 'x', { ifRevision: r3 }), code('CONFLICT'))
  await assert.rejects(db.put('k', 'x', { ifAbsent: true, ifRevision: r3 }), code('INVALID_ARGUMENT'))
  await assert.rejects(db.put('k', 'x', { ifRevision: -1 }), code('INVALID_ARGUMENT'))
  await assert.rejects(db.put('k', 'x', { ifAbsent: 'yes' }), code('INVALID_ARGUMENT'))
  assert.throws(() => db.putSync('k', 'x', { ifAbsent: true }), code('CONFLICT'))
  assert.equal(db.putSync('k', 'v4', { ifRevision: r3 }) > r3, true)
  assert.deepEqual(await db.get('k'), Buffer.from('v4'))
  db.close()
})

test('delete', async () => {
  const db = open(newDir())
  await db.put('a', '1')
  assert.equal(await db.delete('a'), true)
  assert.equal(await db.get('a'), null)
  assert.equal(await db.delete('a'), false)
  const rb = await db.put('b', '2')
  await assert.rejects(db.delete('b', { ifRevision: rb + 100n }), code('CONFLICT'))
  assert.deepEqual(await db.get('b'), Buffer.from('2'))
  assert.equal(await db.delete('b', { ifRevision: rb }), true)
  await assert.rejects(db.delete('b', { ifRevision: rb }), code('CONFLICT'), 'a missing key conflicts with ifRevision')
  db.putSync('c', '3')
  assert.equal(db.deleteSync('c'), true)
  assert.equal(db.deleteSync('c'), false)
  await db.put('a', 'again', { ifAbsent: true })
  assert.deepEqual(await db.get('a'), Buffer.from('again'))
  db.close()
})

test('scan with prefix, reverse, limit and range', async () => {
  const db = open(newDir())
  for (const k of ['a', 'b1', 'b2', 'b3', 'c']) await db.put(k, `v-${k}`)
  assert.deepEqual(scanKeys(await db.scan()), ['a', 'b1', 'b2', 'b3', 'c'])
  const [first] = await db.scan({ prefix: 'b' })
  assert.deepEqual(first, [Buffer.from('b1'), Buffer.from('v-b1')])
  assert.deepEqual(scanKeys(await db.scan({ prefix: 'b' })), ['b1', 'b2', 'b3'])
  assert.deepEqual(scanKeys(await db.scan({ prefix: Buffer.from('b'), reverse: true })), ['b3', 'b2', 'b1'])
  assert.deepEqual(scanKeys(await db.scan({ prefix: 'b', reverse: true, limit: 2 })), ['b3', 'b2'])
  assert.deepEqual(scanKeys(await db.scan({ limit: 2 })), ['a', 'b1'])
  assert.deepEqual(scanKeys(await db.scan({ limit: 0 })), ['a', 'b1', 'b2', 'b3', 'c'], 'limit 0 = no limit')
  assert.deepEqual(scanKeys(await db.scan({ start: 'b1', end: 'b3' })), ['b1', 'b2'], 'start inclusive, end exclusive')
  assert.deepEqual(scanKeys(await db.scan({ start: 'b2' })), ['b2', 'b3', 'c'])
  assert.deepEqual(scanKeys(await db.scan({ end: 'b2' })), ['a', 'b1'])
  assert.deepEqual(scanKeys(await db.scan({ start: 'b1', end: 'c', reverse: true, limit: 2 })), ['b3', 'b2'])
  assert.deepEqual(await db.scan({ start: 'z', end: 'a' }), [])
  assert.deepEqual(await db.scan({ prefix: 'nothing' }), [])
  assert.deepEqual(scanKeys(db.scanSync({ prefix: 'b', limit: 1 })), ['b1'])
  await db.delete('b2')
  assert.deepEqual(scanKeys(await db.scan({ prefix: 'b' })), ['b1', 'b3'], 'deleted keys are skipped')

  await assert.rejects(db.scan({ prefix: 'b', start: 'b1' }), code('INVALID_ARGUMENT'))
  await assert.rejects(db.scan({ limit: -1 }), code('INVALID_ARGUMENT'))
  await assert.rejects(db.scan({ limit: 1.5 }), code('INVALID_ARGUMENT'))
  await assert.rejects(db.scan('b'), code('INVALID_ARGUMENT'))
  assert.throws(() => db.scanSync({ prefix: 'a', end: 'b' }), code('INVALID_ARGUMENT'))
  db.close()
})

test('keys', async () => {
  const db = open(newDir())
  for (const k of ['a', 'b1', 'b2', 'c']) await db.put(k, `v-${k}`)
  const keys = await db.keys({ prefix: 'b' })
  assert.ok(keys.every((k) => Buffer.isBuffer(k)))
  assert.deepEqual(strings(keys), ['b1', 'b2'])
  assert.deepEqual(strings(await db.keys()), ['a', 'b1', 'b2', 'c'])
  assert.deepEqual(strings(db.keysSync({ reverse: true, limit: 1 })), ['c'])
  assert.deepEqual(strings(db.keysSync({ start: 'b', end: 'c' })), ['b1', 'b2'])
  db.close()
})

test('batch is atomic, ordered and all or nothing', async () => {
  const db = open(newDir())
  const goneRevision = await db.put('gone', 'x')
  const results = await db.batch([
    { type: 'put', key: 'b1', value: '1' },
    { type: 'put', key: Buffer.from('b2'), value: Buffer.from('2') },
    { type: 'del', key: 'gone' },
    { type: 'del', key: 'never-existed' },
    { type: 'put', key: 'b1', value: 'overwritten' },
  ])
  assert.equal(results.length, 5)
  assert.equal(typeof results[0], 'bigint')
  assert.equal(typeof results[1], 'bigint')
  assert.equal(results[2], goneRevision, 'a del returns the revision of the deleted record')
  assert.equal(results[3], null, 'a del of a missing key returns null')
  assert.ok(results[4] > results[0], 'a later operation sees the earlier ones')
  assert.deepEqual(await db.get('b1'), Buffer.from('overwritten'))
  assert.equal(await db.get('gone'), null)

  // All or nothing: one invalid operation fails the whole batch.
  await assert.rejects(
    db.batch([
      { type: 'put', key: 'x1', value: '1' },
      { type: 'put', key: '', value: 'empty key' },
      { type: 'del', key: 'b2' },
    ]),
    code('INVALID_ARGUMENT'),
  )
  assert.equal(await db.get('x1'), null)
  assert.deepEqual(await db.get('b2'), Buffer.from('2'))
  await assert.rejects(
    db.batch([
      { type: 'put', key: 'x2', value: '1' },
      { type: 'put', key: 'k'.repeat(5000), value: 'key over 4096 bytes' },
    ]),
    code('INVALID_ARGUMENT'),
  )
  assert.equal(await db.get('x2'), null)

  await assert.rejects(db.batch([{ type: 'upsert', key: 'a', value: 'b' }]), code('INVALID_ARGUMENT'))
  await assert.rejects(db.batch([{ type: 'put', key: 'a' }]), code('INVALID_ARGUMENT'))
  await assert.rejects(db.batch([{ type: 'del' }]), code('INVALID_ARGUMENT'))
  await assert.rejects(db.batch('not an array'), code('INVALID_ARGUMENT'))
  assert.equal(await db.get('a'), null)
  assert.deepEqual(await db.batch([]), [])

  const sync = db.batchSync([
    { type: 'put', key: 's1', value: 'x' },
    { type: 'del', key: 's1' },
  ])
  assert.equal(sync[1], sync[0], 'the del sees the put of the same batch')
  assert.equal(db.getSync('s1'), null)
  assert.throws(() => db.batchSync([{ type: 'put', key: '', value: 'x' }]), code('INVALID_ARGUMENT'))
  db.close()
})

test('reopen: data persists in both durability modes', async () => {
  const dir = newDir()
  let db = open(dir)
  await db.put('async', 'yes')
  db.putSync('sync', 'yes')
  await db.batch([{ type: 'put', key: 'batch', value: 'yes' }])
  db.close()

  db = open(dir, { durability: 'buffered' })
  assert.equal(db.durability, 'buffered')
  for (const k of ['async', 'sync', 'batch']) assert.deepEqual(await db.get(k), Buffer.from('yes'), k)
  await db.put('buffered-synced', 'yes')
  await db.sync()
  await db.put('buffered-closed', 'yes') // close() makes it durable
  db.close()

  db = open(dir)
  for (const k of ['async', 'sync', 'batch', 'buffered-synced', 'buffered-closed']) {
    assert.deepEqual(await db.get(k), Buffer.from('yes'), k)
  }
  db.close()
})

test('close semantics', async () => {
  const dir = newDir()
  const db = open(dir)
  assert.equal(db.closed, false)
  assert.equal(db.durability, 'immediate')
  assert.ok(isAbsolute(db.path))
  assert.equal(realpathSync.native(db.path), realpathSync.native(dir))
  assert.throws(() => open(dir), code('ALREADY_OPEN'), 'one handle per directory per process')

  const before = [db.put('before-close', 'x'), db.get('before-close'), db.scan()]
  db.close()
  assert.equal(db.closed, true)
  const [revision] = await Promise.all(before)
  assert.equal(typeof revision, 'bigint', 'calls made before close() complete')

  await assert.rejects(db.get('x'), code('CLOSED'))
  await assert.rejects(db.put('x', 'y'), code('CLOSED'))
  await assert.rejects(db.delete('x'), code('CLOSED'))
  await assert.rejects(db.scan(), code('CLOSED'))
  await assert.rejects(db.keys(), code('CLOSED'))
  await assert.rejects(db.batch([]), code('CLOSED'))
  await assert.rejects(db.sync(), code('CLOSED'))
  assert.throws(() => db.getSync('x'), code('CLOSED'))
  assert.throws(() => db.putSync('x', 'y'), code('CLOSED'))
  assert.throws(() => db.scanSync(), code('CLOSED'))
  db.close() // idempotent

  const again = open(dir) // the directory is released at once
  assert.deepEqual(await again.get('before-close'), Buffer.from('x'))
  again.close()

  assert.throws(() => open(dir, { durability: 'sometimes' }), code('INVALID_ARGUMENT'))
  assert.throws(() => open(dir, 'buffered'), code('INVALID_ARGUMENT'))
  assert.throws(() => open(''), code('INVALID_ARGUMENT'))
  assert.throws(() => open(42), code('INVALID_ARGUMENT'))
  assert.throws(() => new Database())
})

test('1000 concurrent puts with Promise.all', async () => {
  for (const durability of ['immediate', 'buffered']) {
    const db = open(newDir(), { durability })
    const key = (i) => `key-${String(i).padStart(4, '0')}`
    const revisions = await Promise.all(Array.from({ length: 1000 }, (_, i) => db.put(key(i), `value-${i}`)))
    assert.ok(revisions.every((r) => typeof r === 'bigint'))
    assert.equal(new Set(revisions.map(String)).size, 1000, `${durability}: distinct revisions`)
    assert.equal((await db.keys({ prefix: 'key-' })).length, 1000)
    const values = await Promise.all(Array.from({ length: 1000 }, (_, i) => db.get(key(i))))
    values.forEach((v, i) => assert.equal(v.toString(), `value-${i}`))
    // Mixed concurrent traffic on the same keys.
    await Promise.all([
      ...Array.from({ length: 200 }, (_, i) => db.delete(key(i))),
      ...Array.from({ length: 200 }, (_, i) => db.put(key(1000 + i), 'new')),
      ...Array.from({ length: 200 }, (_, i) => db.get(key(500 + i))),
      db.scan({ prefix: 'key-', limit: 10 }),
    ])
    assert.equal((await db.keys({ prefix: 'key-' })).length, 1000)
    db.close()
  }
})

test('chat key helpers', async () => {
  const key = messageKey(42n, 7n)
  assert.ok(Buffer.isBuffer(key))
  assert.deepEqual(key, Buffer.from('000000000000002a0000000000000007', 'hex'))
  assert.deepEqual(messageKey(42, 7), key, 'numbers are accepted')
  assert.deepEqual(parseMessageKey(key), [42n, 7n])
  assert.equal(parseMessageKey(Buffer.alloc(3)), null)
  const max = 2n ** 64n - 1n
  assert.deepEqual(parseMessageKey(messageKey(max, max)), [max, max])
  assert.deepEqual(channelPrefix(42n), key.subarray(0, 8))
  assert.throws(() => messageKey(-1, 1), code('INVALID_ARGUMENT'))
  assert.throws(() => messageKey(2n ** 64n, 1n), code('INVALID_ARGUMENT'))
  assert.throws(() => channelPrefix(1.5), code('INVALID_ARGUMENT'))

  const db = open(newDir())
  for (let id = 1n; id <= 10n; id++) await db.put(messageKey(42n, id), `m${id}`)
  await db.put(messageKey(43n, 1n), 'another channel')
  const latest = await db.scan({ prefix: channelPrefix(42n), reverse: true, limit: 3 })
  assert.deepEqual(latest.map(([k]) => parseMessageKey(k)[1]), [10n, 9n, 8n])
  assert.deepEqual(latest[0][1], Buffer.from('m10'))
  db.close()
})

test('buffered writes survive a process exit without close()', () => {
  for (const ending of ['/* natural exit */', 'process.exit(0)']) {
    const dir = newDir()
    child(`
      const { open } = require(${JSON.stringify(indexPath)})
      const db = open(${JSON.stringify(dir)}, { durability: 'buffered' })
      ;(async () => {
        for (let i = 0; i < 300; i++) await db.put('k' + i, 'v' + i)
        ${ending}
      })()
    `)
    const out = child(`
      const { open } = require(${JSON.stringify(indexPath)})
      const db = open(${JSON.stringify(dir)})
      let found = 0
      for (let i = 0; i < 300; i++) if (String(db.getSync('k' + i)) === 'v' + i) found++
      db.close()
      console.log(found)
    `)
    assert.equal(out.trim(), '300', ending)
  }
})

test('a garbage-collected handle is closed and releases its directory', () => {
  const dir = newDir()
  const out = child(
    `
      const { open } = require(${JSON.stringify(indexPath)})
      let db = open(${JSON.stringify(dir)}, { durability: 'buffered' })
      db.putSync('kept', 'yes')
      db = null
      const retry = (n) => {
        globalThis.gc()
        setImmediate(() => {
          try {
            const again = open(${JSON.stringify(dir)})
            console.log(String(again.getSync('kept')))
            again.close()
          } catch (e) {
            if (e.code === 'ALREADY_OPEN' && n > 0) return setTimeout(() => retry(n - 1), 20)
            throw e
          }
        })
      }
      retry(50)
    `,
    ['--expose-gc'],
  )
  assert.equal(out.trim(), 'yes')
})
