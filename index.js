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

    this._corestores = new Map()
    this._ready = Promise.all([
      this.networking.ready()
    ])
  }

  ready () {
    return this._ready
  }

  get (name, coreOpts, opts = {}) {
    const self = this

    const storage = path => this.storage(name + '/' + path)
    const store = corestore(storage, { ...this.opts, coreOpts })
    const { get, replicate, close } = store
    var mainKey, mainKeyString

    return {
      get: wrappedGet,
      close: wrappedClose,
      replicate,
    }

    function wrappedClose (cb) {
      close(err => {
        if (err) return cb(err)
        if (opts.seed !== false) this.networking.unseed(mainKeyString)
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
          seed: opts.seed !== false,
          writable: core.writable,
          sparse: !!coreOpts.sparse,
          valueEncoding: coreOpts.valueEncoding,
          name: coreOpts.name
        })

        batch.push({ type: 'put', key: encodedKey, value })
        batch.push({ type: 'put', key: encodedDKey, value })

        if (coreOpts.main) {
          mainKey = core.key
          mainKeyString = encodedKey
          self._corestores.set(encodedKey, store)
          if (opts.seed !== false) self.networking.seed(core.discoveryKey, replicate)
        } else {
          batch.push({ type: 'put', key: mainKeyString + '/' + encodedKey, value })
        }
        if (coreOpts.name) batch.push({ type: 'put', key: mainKeyString + '/' + coreOpts.name, value })

        self.db.batch(batch, err => {
          if (err) this.emit('error', err)
        })
      })
      return core
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
    return Promise.all([
      this.networking.close(),
      this.db.close(),
      [...this._corestores].map((_, store) => new Promise((resolve, reject) => {
        store.close(err => {
          if (err) return reject(err)
          return resolve()
        })
      }))
    ])
  }
}

module.exports = RandomAccessMegastore
