const { EventEmitter } = require('events')
const datEncoding = require('dat-encoding')
const sub = require('subleveldown')
const crypto = require('hypercore-crypto')
const corestore = require('random-access-corestore')
const collect = require('stream-collector')

const CORESTORE_PREFIX = 'corestore/'
const DISCOVERABLE_PREFIX = 'discoverable/'
const SUBCORE_PREFIX = 'subcore/'
const CORE_PREFIX = 'core/'

class Megastore extends EventEmitter {
  constructor (storage, db, networking, opts) {
    if (typeof storage !== 'function') storage = path => storage(path)
    super()

    this.opts = opts
    this.storage = storage
    this.db = db
    this.networking = networking

    this._id = Date.now()

    if (this.networking) {
      this.networking.on('error', err => this.emit('error', err))
      this.networking.setReplicatorFactory(async dkey => {
        const existing = this._corestoresByDKey.get(dkey)
        if (existing) return existing.replicate
        try {
          const { name, key, opts: coreOpts } = await this._storeIndex.get('corestore/' + dkey)
          if (coreOpts.seed === false) return null

          const store = this._corestoresByDKey.get(dkey) || this.get(name)

          // Inflating the default hypercore here will set the default key and bootstrap replication.
          store.default(datEncoding.decode(key))

          return store.replicate
        } catch (err) {
          if (!err.notFound) throw err
          return null
        }
      })
    }

    this._storeIndex = sub(this.db, 'stores', { valueEncoding: 'json' })
    this._keyIndex = sub(this.db, 'keys', { valueEncoding: 'json' })

    // The megastore duplicates storage across corestore instances. This top-level cache allows us to
    // reuse in-memory hypercores.
    this._cores = new Map()
    this._corestores = new Map()
    this._corestoresByDKey = new Map()

    // The set of seeded discovery keys. Created in _reseed.
    this._seeding = new Set()
    // Default feed keys are cached here so that they can be fetched synchronously. Created in _loadKeys.
    this._keys = null

    this.isReady = false
    this._ready = async () => {
      await this._loadKeys()
      await (this.networking ? this._reseed() : Promise.resolve())
    }
  }

  _collect(index, prefix) {
    return new Promise((resolve, reject) => {
      if (prefix) var opts = {
        gt: prefix,
        lt: prefix + String.fromCharCode(65535)
      }
      collect(index.createReadStream(opts), (err, list) => {
        if (err) return reject(err)
        return resolve(list)
      })
    })
  }

  async _loadKeys () {
    const keyList = await this._collect(this._keyIndex)
    this._keys = new Map()
    for (const { key, value } of keyList) {
      this._keys.set(key, {
        publicKey: Buffer.from(value.publicKey, 'hex'),
        secretKey: Buffer.from(value.secretKey, 'hex')
      })
    }
  }

  _seed (dkey) {
    this._seeding.add(dkey)
    this.networking.seed(datEncoding.decode(dkey))
  }

  _unseed (dkey) {
    this._seeding.delete(dkey)
    this.networking.unseed(datEncoding.decode(dkey))
  }

  async _seedDiscoverableSubfeeds (dkey) {
    const feedList = await this._collect(this._storeIndex, DISCOVERABLE_PREFIX + dkey + '/')
    for (const { key, value } of feedList) {
      this._seed(key.split('/')[2])
    }
  }

  async _unseedDiscoverableSubfeeds (dkey) {
    const feedList = await this._collect(this._storeIndex, DISCOVERABLE_PREFIX + dkey + '/')
    for (const { key, value } of feedList) {
      this._unseed(key.split('/')[2])
    }
  }

  async _reseed () {
    const storeList = await this._collect(this._storeIndex, CORESTORE_PREFIX)
    for (let { key, value } of storeList) {
      if (value.seed === false) continue
      key = key.slice(10)
      try {
        this._seed(key)
      } catch (err) {
        // If it could not be seeded, then it was indexed by name
        continue
      }
    }
  }

  async ready () {
    if (this.isReady) return
    await this._ready()
    this.isReady = true
  }

  isSeeding (dkey) {
    return this._seeding.has(dkey)
  }

  get (name, opts = {}) {
    const self = this
    var mainDiscoveryKeyString, mainKeyString

    const store = corestore(this.storage, { ...this.opts, ...opts })
    const {
      get: innerGet,
      replicate: innerReplicate,
      default: innerGetDefault,
      list: innerList
    } = store

    const wrappedStore = {
      default: wrappedDefault,
      get: wrappedGet,
      replicate: innerReplicate,
      list: innerList,
      close
    }

    return wrappedStore

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

      const value = {
        key: core.key,
        seed: opts.seed !== false,
        writable: core.writable,
      }

      batch.push({ type: 'put', key: CORE_PREFIX + encodedDiscoveryKey, value })
      self._cores.set(encodedKey, { core, refs: [name] })

      if (coreOpts.default || coreOpts.discoverable) {
        const record = { name, opts: { ...opts, ...coreOpts }, key: encodedKey, discoveryKey: encodedDiscoveryKey }

        batch.push({ type: 'put', key: CORESTORE_PREFIX + name, value: record })
        batch.push({ type: 'put', key: CORESTORE_PREFIX + encodedDiscoveryKey, value: record })

        self._corestores.set(encodedKey, wrappedStore)
        self._corestoresByDKey.set(encodedDiscoveryKey, wrappedStore)

        if (coreOpts.default) {
          mainDiscoveryKeyString = encodedDiscoveryKey
          mainKeyString = encodedKey
          self._corestores.set(name, wrappedStore)
        } else {
          batch.push({ type: 'put', key: DISCOVERABLE_PREFIX + mainDiscoveryKeyString + '/' + encodedDiscoveryKey, value: {}})
        }

        if (self.networking && opts.seed !== false) {
          self._seed(encodedDiscoveryKey)
        }

      } else {
        batch.push({ type: 'put', key: SUBCORE_PREFIX +  mainDiscoveryKeyString + '/' + encodedDiscoveryKey, value: {}})
      }

      self._storeIndex.batch(batch, err => {
        if (err) this.emit('error', err)
      })
    }

    function close (cb) {
      if (self.networking && mainDiscoveryKeyString && self.isSeeding(mainDiscoveryKeyString)) {
        self._unseed(mainDiscoveryKeyString)
      }

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
        self._corestoresByDKey.delete(mainDiscoveryKeyString)
        return cb(null)
      }
    }

    function wrappedDefault (coreOpts) {
      if (coreOpts instanceof Buffer) coreOpts = { key: coreOpts }
      const key = coreOpts && coreOpts.key

      if (!key) {
        var keyPair = crypto.keyPair()
        var { publicKey, secretKey } = keyPair
        self._keys.set(name, keyPair)
        self._keyIndex.put(name, {
          secretKey: keyPair.secretKey.toString('hex'),
          publicKey: keyPair.publicKey.toString('hex')
        }, err => {
          if (err) return self.emit('error', err)
        })
      } else {
        keyPair = self._keys.get(name)
        if (keyPair) secretKey = keyPair.secretKey
      }
      coreOpts = { ...coreOpts, secretKey, keyPair, default: true }
      return getCore(innerGetDefault, coreOpts)
    }

    function wrappedGet (coreOpts = {}) {
      if (coreOpts instanceof Buffer) coreOpts = { key: coreOpts }
      return getCore(innerGet, coreOpts)
    }
  }

  async info (opts) {

  }

  async seed (idx) {
    if (idx instanceof Buffer) idx = datEncoding.encode(idx)

    const record = await this._storeIndex.get(CORESTORE_PREFIX + idx)
    const dkey = datEncoding.decode(record.discoveryKey)

    if (!this.networking || this.isSeeding(record.discoveryKey)) return
    record.opts.seed = true

    await this._storeIndex.batch([
      { type: 'put', key: CORESTORE_PREFIX + idx, value: record },
      { type: 'put', key: CORESTORE_PREFIX + record.discoveryKey, value: record }
    ])

    this._seeding.add(record.discoveryKey)
    this.networking.seed(dkey)

    await this._seedDiscoverableSubfeeds(record.discoveryKey)
  }

  async unseed (idx) {
    if (idx instanceof Buffer) idx = datEncoding.encode(idx)

    const record = await this._storeIndex.get(CORESTORE_PREFIX + idx)
    const dkey = datEncoding.decode(record.discoveryKey)

    if (!this.networking || !this.isSeeding(record.discoveryKey)) return
    record.opts.seed = false

    await this._storeIndex.batch([
      { type: 'put', key: CORESTORE_PREFIX + idx, value: record },
      { type: 'put', key: CORESTORE_PREFIX + record.discoveryKey, value: record }
    ])

    this._seeding.delete(record.discoveryKey)
    this.networking.unseed(dkey)

    await this._unseedDiscoverableSubfeeds(record.discoveryKey)
  }

  async delete (opts) {

  }

  async list () {

  }

  async close () {
    return Promise.all([
      this.networking ? this.networking.close() : Promise.resolve(),
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
