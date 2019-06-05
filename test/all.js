const test = require('tape')

const ram = require('random-access-memory')
const memdb = require('memdb')

const SwarmNetworker = require('megastore-swarm-networking')
const Megastore = require('..')

test('replication of two corestores', async t => {
  const megastore1 = new Megastore(ram, memdb(), new SwarmNetworker({ port: 3006 }))
  const megastore2 = new Megastore(ram, memdb(), new SwarmNetworker({ port: 3007 }))
  await megastore1.ready()
  await megastore2.ready()

  megastore1.on('error', err => t.fail(err))
  megastore2.on('error', err => t.fail(err))

  const cs1 = megastore1.get('cs1')
  const cs2 = megastore2.get('cs2')

  await new Promise(resolve => {
    const core1 = cs1.default()
    core1.ready(err => {
      t.error(err, 'no error')
      const core2 = cs2.default({ key: core1.key })
      core2.ready(err => {
        t.error(err, 'no error')
        append(core1, core2)
      })
    })

    function append (core1, core2) {
      core1.append('hello', err => {
        t.error(err, 'no error')
        core2.get(0, (err, contents) => {
          t.error(err, 'no error')
          t.same(contents, Buffer.from('hello'))
          return resolve()
        })
      })
    }
  })

  await megastore1.close()
  await megastore2.close()

  t.end()
})

test('replication of two corestores with multiple channels', async t => {
  const megastore1 = new Megastore(ram, memdb(), new SwarmNetworker({ port: 3006 }))
  const megastore2 = new Megastore(ram, memdb(), new SwarmNetworker({ port: 3007 }))
  await megastore1.ready()
  await megastore2.ready()

  megastore1.on('error', err => t.fail(err))
  megastore2.on('error', err => t.fail(err))

  const cs1 = megastore1.get('cs1')
  const cs2 = megastore2.get('cs2')

  await new Promise(resolve => {
    const core1 = cs1.default()
    const core2 = cs1.get()
    core1.ready(err => {
      t.error(err, 'no error')
      cs2.default({ key: core1.key })
      const core3 = cs2.get({ key: core2.key })
      core3.ready(err => {
        t.error(err, 'no error')
        append(core2, core3)
      })
    })

    function append (core1, core2) {
      core1.append('hello', err => {
        t.error(err, 'no error')
        core2.get(0, (err, contents) => {
          t.error(err, 'no error')
          t.same(contents, Buffer.from('hello'))
          return resolve()
        })
      })
    }
  })

  await megastore1.close()
  await megastore2.close()

  t.end()
})

test('replicates across restarts', async t => {
  const megastore1 = new Megastore(ram, memdb(), new SwarmNetworker({ port: 3006 }))
  const megastore2 = new Megastore(ram, memdb(), new SwarmNetworker({ port: 3007 }))
  var defaultKey

  await megastore1.ready()
  await megastore2.ready()
  megastore1.on('error', err => t.fail(err))
  megastore2.on('error', err => t.fail(err))

  var cs1 = megastore1.get('cs1')
  const cs2 = megastore2.get('cs2')

  await createAndClose()
  cs1 = megastore1.get('cs1')
  await testAppend()

  await megastore1.close()
  await megastore2.close()

  t.end()

  function createAndClose () {
    return new Promise(resolve => {
      const defaultCore = cs1.default()
      defaultCore.ready(err => {
        t.error(err, 'no error')
        defaultKey = defaultCore.key
        cs1.close(err => {
          t.error(err, 'no error')
          return resolve()
        })
      })
    })
  }

  function testAppend () {
    return new Promise(resolve => {
      const core1 = cs1.default(defaultKey)
      core1.ready(err => {
        t.error(err, 'no error')
        const core2 = cs2.default({ key: core1.key })
        core1.ready(err => {
          t.error(err, 'no error')
          append(core1, core2)
        })
      })

      function append (core1, core2) {
        core1.append('hello', err => {
          t.error(err, 'no error')
          core2.get(0, (err, contents) => {
            t.error(err, 'no error')
            t.same(contents, Buffer.from('hello'))
            return resolve()
          })
        })
      }
    })
  }
})

test('reuses hypercores across corestores', async t => {
  const megastore = new Megastore(ram, memdb(), new SwarmNetworker({ port: 3006 }))
  await megastore.ready()
  megastore.on('error', err => t.fail(err))

  const cs1 = megastore.get('cs1')
  const cs2 = megastore.get('cs2')

  const core1 = cs1.default()
  await new Promise(resolve => {
    core1.ready(err => {
      t.error(err, 'no error')
      const core2 = cs2.default(core1.key)
      core2.ready(err => {
        t.error(err, 'no error')
        // core2 will only be writable if it's the same hypercore, since cs2 does not have access to its key.
        t.true(core2.writable)
        resolve()
      })
    })
  })

  await megastore.close()
  t.end()
})

test('does not replicate a corestore that\'s not seeded')
test('unseeds a replicating corestore')
test('lists all corestores')
