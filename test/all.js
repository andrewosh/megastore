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

  const cs1 = megastore1.get('cs1')
  const cs2 = megastore2.get('cs2')

  await new Promise(resolve => {
    const core1 = cs1.get({ main: true })
    core1.ready(err => {
      t.error(err, 'no error')
      const core2 = cs2.get({ key: core1.key, main: true })
      core2.ready(err => {
        t.error(err, 'no error')
        append(core1, core2)
      })
    })

    function append(core1, core2) {
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

  const cs1 = megastore1.get('cs1')
  const cs2 = megastore2.get('cs2')

  await new Promise(resolve => {
    const core1 = cs1.get({ main: true })
    const core2 = cs1.get({ name: 'core2' })
    core1.ready(err => {
      t.error(err, 'no error')
      cs2.get({ key: core1.key, main: true })
      const core3 = cs2.get({ key: core1.key })
      core5.ready(err => {
        t.error(err, 'no error')
        append(core3, core5)
      })
    })

    function append(core1, core2) {
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

test('does not replicate a corestore that\'s not seeded')
test('unseeds a replicating corestore')
test('lists all corestores')
