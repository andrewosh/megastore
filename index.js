const { EventEmitter } = require('events')
const datEncoding = require('dat-encoding')
const corestore = require('random-access-corestore')

const { Core } = require('./lib/messages')

class RandomAccessMegastore extends EventEmitter {
  constructor(storage, db, networking, opts) {
    if (typeof storage !== 'function') storage = path => storage(path)
    super()

    this.storage = storage
    this.db = db
    this.networking = networking
    this.opts = opts
  }

  async get (name, coreOpts, opts = {}) {
    const storage = path => this.storage(name + '/' + path)
    const store = corestore(storage, { ...this.opts, coreOpts })
    const { get, replicate, close } = store
    var encodedMainKey

    // TODO: Support encrypted replication.
    if (opts.seed !== false) this.networking.seed(store)

    return {
      get: wrappedGet,
      close: wrappedClose,
      replicate,
    }

    function wrappedClose (cb) {
      close(err => {
        if (err) return cb(err)
        if (opts.seed !== false) this.networking.unseed(store)
      })
    }

    function wrappedGet (coreOpts = {}) {
      const core = get(coreOpts)
      core.on('ready', () => {
        const batch = []
        const encodedKey = datEncoding.encode(core.key)
        const encodedDKey = datEncoding.encode(core.discoveryKey)

        const value = Core.encode({
          key: core.key,
          seed: opts.seed,
          writable: core.writable,
          sparse: coreOpts.sparse,
          valueEncoding: coreOpts.valueEncoding,
          name: coreOpts.name
        })

        batch.push({ type: 'put', key: encodedKey, value })
        batch.push({ type: 'put', key: encodedDKey, value })

        if (coreOpts.main) encodedMainKey = encodedKey
        else batch.push({ type: 'put', key: encodedMainKey + '/' + encodedKey, value })
        if (coreOpts.name) batch.push({ type: 'put', key: encodedMainKey + '/' + coreOpts.name, value })

        this.db.batch(batch, err => {
          if (err) this.emit('error', err)
        })
      })
    }
  }

  async info (opts) {

  }

  async update (opts) {

  }

  async delete (opts) {

  }

  async list () {

  }

  async close () {

  }
}

module.exports = RandomAccessMegastore
