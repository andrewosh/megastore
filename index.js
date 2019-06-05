const { EventEmitter } = require('events')
const datEncoding = require('dat-encoding')
const sub = require('subleveldown')
const crypto = require('hypercore-crypto')
const corestore = require('random-access-corestore')
const collect = require('stream-collector')

const { Core } = require('./lib/messages')

class Megastore extends EventEmitter {
  constructor (storage, db, networking, opts) {
    if (typeof storage !== 'function') storage = path => storage(path)
    super()

    this.opts = opts
    this.storage = storage
    this.db = db
    this.networking = networking
    if (this.networking) this.networking.on('error', err => this.emit('error', err))

    this._coreIndex = sub(this.db, 'cores')
    this._keyIndex = sub(this.db, 'keys')

    // The megastore duplicates storage across corestore instances. This top-level cache allows us to
    // reuse in-memory hypercores.
    this._cores = new Map()
    // Default feed keys are cached here so that they can be fetched synchronously. Created in _loadKeys.
    this._keys = null

    this._corestores = new Map()
    this._ready = Promise.all([
      this._loadKeys(),
      this.networking ? this.networking.ready() : Promise.resolve()
    ])
  }

  _loadKeys () {
    return new Promise((resolve, reject) => {
      collect(this._keyIndex.createReadStream(), (err, list) => {
        if (err) return reject(err)
        this._keys = new Map(list)
        return resolve()
      })
    })
  }

  ready () {
    return this._ready
  }

  get (name, opts = {}) {
    const self = this
    var mainDiscoveryKey, mainKeyString

    const store = corestore(this.storage, { ...this.opts, ...opts })
    const {
      get: innerGet,
      replicate: innerReplicate,
      default: innerGetDefault,
      list: innerList
    } = store

    return {
      default: wrappedDefault,
      get: wrappedGet,
      replicate: innerReplicate,
      list: innerList,
      close
    }

    function getCore (getter, coreOpts) {
      if (coreOpts && coreOpts.key) {
        const existing = self._cores.get(datEncoding.encode(coreOpts.key))
        if (existing) {
          if (existing.refs.indexOf(name) === -1) existing.refs.push(name)
          return existing.core
        }
      }
      const core = getter(coreOpts)
      core.on('ready', () => processCore(core, coreOpts))
      return core
    }

    function processCore (core, coreOpts) {
      const batch = []
      const encodedKey = datEncoding.encode(core.key)
      const encodedDiscoveryKey = datEncoding.encode(core.discoveryKey)

      const value = Core.encode({
        key: core.key,
        seed: opts.seed !== false,
        writable: core.writable,
        sparse: !!coreOpts.sparse,
        valueEncoding: coreOpts.valueEncoding
      })

      batch.push({ type: 'put', key: encodedKey, value })
      batch.push({ type: 'put', key: encodedDiscoveryKey, value })
      self._cores.set(encodedKey, { core, refs: [name] })

      if (coreOpts.default) {
        mainDiscoveryKey = encodedDiscoveryKey
        mainKeyString = encodedKey
        self._corestores.set(mainKeyString, store)
        if (opts.seed !== false && self.networking) self.networking.seed(core.discoveryKey, innerReplicate)
      } else {
        batch.push({ type: 'put', key: mainKeyString + '/' + encodedKey, value })
      }

      self._coreIndex.batch(batch, err => {
        if (err) this.emit('error', err)
      })
    }

    function close (cb) {
      if (opts.seed !== false && mainDiscoveryKey && self.networking) self.networking.unseed(mainDiscoveryKey)

      const cores = innerList()
      var pending = 0

      // TODO: How do we mark the corestore as closed when the close behavior is here?

      for (let [key, core] of cores) {
        const outerCore = self._cores.get(key)
        if (!outerCore) continue

        const { refs } = outerCore
        refs.splice(refs.indexOf(name), 1)

        if (!refs.length) {
          pending++
          core.close(err => {
            if (err) return cb(err)
            self._cores.delete(key)
            if (!--pending) return onclosed()
          })
        }
      }
      if (!pending) return process.nextTick(onclosed)

      function onclosed () {
        self._corestores.delete(mainKeyString)
        return cb(null)
      }
    }

    function wrappedDefault (coreOpts) {
      if (coreOpts instanceof Buffer) coreOpts = { key: coreOpts }

      if (!coreOpts || !coreOpts.key) {
        const keyPair = crypto.keyPair()
        var { publicKey, secretKey } = keyPair
        self._keys.set(name, keyPair)
        self._keyIndex.put(name, secretKey, err => {
          if (err) return self.emit('error', err)
        })
        coreOpts = { key: publicKey }
      } else {
        const storedKey = self._keys.get(name)
        if (storedKey) secretKey = storedKey.secretKey
      }
      coreOpts = { ...coreOpts, secretKey, default: true }
      return getCore(innerGetDefault, coreOpts)
    }

    function wrappedGet (coreOpts = {}) {
      if (coreOpts instanceof Buffer) coreOpts = { key: coreOpts }
      return getCore(innerGet, coreOpts)
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
      this.networking ? this.networking.close() : Promise.resolve(),
      this.db.close(),
      [...this._corestores].map(([, store]) => new Promise((resolve, reject) => {
        store.close(err => {
          if (err) return reject(err)
          return resolve()
        })
      }))
    ])
  }
}

module.exports = Megastore
