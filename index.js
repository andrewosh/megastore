const { EventEmitter } = require('events')
const datEncoding = require('dat-encoding')
const sub = require('subleveldown')
const crypto = require('hypercore-crypto')
const corestore = require('random-access-corestore')
const collect = require('stream-collector')
const duplexify = require('duplexify')

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
        console.log('GOT DKEY REQUEST:', dkey)
        try {
          var store = await this._getPrimaryStore(dkey)
        } catch (err) {
          console.log('ERROR HERE:', err)
        }
        return store.replicate
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

  async _getPrimaryStore (dkey) {
    const { name, key, coreOpts } = await this._storeIndex.get(CORESTORE_PREFIX + dkey)
    const cached = this._corestores.get(name)
    console.log('CACHED HERE??', !!cached, 'name:', name, 'key:', key)
    if (cached) return cached

    const store = this.get(name, { ...this.opts, ...coreOpts })
    store.default(datEncoding.decode(key))

    return store
  }

  async _getAllCorestores (dkey) {
    const prefix = CORESTORE_PREFIX + dkey + '/'
    const records = await this._collect(this._storeIndex, prefix)
    const cache = this._corestoresByDKey.get(dkey)

    const stores = []

    for (const { value: { name, key, opts: coreOpts } } of records) {
      const cached = cache && cache.get(name)
      if (cached) {
        stores.push(cached)
        continue
      }

      const store = this.get(name, coreOpts)
      // Inflating the default hypercore here will set the default key and bootstrap replication.
      store.default(datEncoding.decode(key))
      stores.push(store)
    }

    return stores
  }

  async _getDiscoverableKeys (dkey) {
    const feedList = await this._collect(this._storeIndex, DISCOVERABLE_PREFIX + dkey + '/')
    const keys = [dkey]
    for (const { key, value } of feedList) {
      keys.push(key.split('/')[2])
    }
    return keys
  }

  _seed (dkey) {
    // Ensure that this is a valid hypercore key string.
    dkey = datEncoding.encode(dkey)

    console.error('SEEDING DKEY:', dkey)
    this._seeding.add(dkey)
    this.networking.seed(datEncoding.decode(dkey))
  }

  _unseed (dkey) {
    // Ensure that this is a valid hypercore key string.
    dkey = datEncoding.encode(dkey)

    console.error('UNSEEDING DKEY:', dkey)
    this._seeding.delete(dkey)
    this.networking.unseed(datEncoding.decode(dkey))
  }

  async _seedDiscoverableSubfeeds (dkey) {
    const keyList = await this._getDiscoverableKeys(dkey)
    for (const key of keyList) {
      this._seed(key)
    }
  }

  async _unseedDiscoverableSubfeeds (dkey) {
    const keyList = await this._getDiscoverableKeys(dkey)
    for (const key of keyList) {
      this._unseed(key)
    }
  }

  async _reseed () {
    const storeList = await this._collect(this._storeIndex, CORESTORE_PREFIX)
    for (let { key, value } of storeList) {
      if (value.seed === false) continue
      try {
        this._seed(value.discoveryKey)
      } catch (err) {
        // If it could not be seeded, then it was indexed by name
        console.error('RESEED ERR:', err)
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
      replicate: wrappedReplicate,
      close,
      name,
      _inner: store
    }

    return wrappedStore

    function getCore (getter, coreOpts) {
      if (coreOpts && coreOpts.key) {
        const existing = self._cores.get(datEncoding.encode(coreOpts.key))
        if (existing) {
          var { core, refs } = existing
          const dkey = datEncoding.encode(core.discoveryKey)
          if (refs.indexOf(name) === -1) refs.push(name)
          if (!self.isSeeding(dkey) && (opts.seed !== false) && coreOpts.discoverable) {
            self._seed(dkey)
          }
          return core
        }
      }
      core = getter(coreOpts)
      core.on('ready', () => processCore(core, coreOpts))
      return core
    }

    function processCore (core, coreOpts) {
      console.log('**** PROCESSING CORE:', core)
      const batch = []
      const encodedKey = datEncoding.encode(core.key)
      const encodedDiscoveryKey = datEncoding.encode(core.discoveryKey)

      const value = {
        key: core.key,
        seed: opts.seed !== false,
        sparse: opts.sparse !== false,
        writable: core.writable,
      }

      batch.push({ type: 'put', key: CORE_PREFIX + encodedDiscoveryKey, value })
      self._cores.set(encodedKey, { core, refs: [name] })

      if (coreOpts.default || coreOpts.discoverable) {
        const record = { name, opts: { ...opts, ...coreOpts }, key: encodedKey, discoveryKey: encodedDiscoveryKey }

        batch.push({ type: 'put', key: CORESTORE_PREFIX + name, value: record })
        batch.push({ type: 'put', key: CORESTORE_PREFIX + encodedDiscoveryKey + '/' + name, value: record })
        batch.push({ type: 'put', key: CORESTORE_PREFIX + encodedDiscoveryKey, value: record })

        var dkeyMap = self._corestoresByDKey.get(encodedDiscoveryKey)
        if (!dkeyMap) {
          dkeyMap = new Map()
          self._corestoresByDKey.set(encodedDiscoveryKey, dkeyMap)
        }
        dkeyMap.set(name, wrappedStore)

        if (coreOpts.default) {
          mainDiscoveryKeyString = encodedDiscoveryKey
          mainKeyString = encodedKey
          self._corestores.set(encodedKey, wrappedStore)
          self._corestores.set(name, wrappedStore)
          batch.push({ type: 'put', key: CORESTORE_PREFIX + mainDiscoveryKeyString, value: record })
        } else {
          batch.push({ type: 'put', key: DISCOVERABLE_PREFIX + mainDiscoveryKeyString + '/' + encodedDiscoveryKey, value: {}})
        }
        if (self.networking && opts.seed !== false) {
          self._seed(encodedDiscoveryKey)
        }
      } else {
        batch.push({ type: 'put', key: SUBCORE_PREFIX +  mainDiscoveryKeyString + '/' + encodedDiscoveryKey, value: {}})
      }

      if (self.networking) {
        self._getDiscoverableKeys(mainDiscoveryKeyString)
          .then(keys => self.networking.injectCore(core, keys))
          .catch(err => self.emit('error', err))
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

    function wrappedReplicate (replicationOpts) {
      console.log('IN WRAPPED REPLICATE')
      const outerStream = replicationOpts && replicationOpts.stream
      console.log('BEFORE INNER REPLICATE')
      const stream = innerReplicate({ ...replicationOpts, stream: outerStream })

      self._getAllCorestores(mainDiscoveryKeyString)
        .then(otherStores => {
          console.log(name, 'IN WRAPPED REPLICATE, otherStores:', otherStores.map(os => os.name))
          for (const store of otherStores) {
            // TODO: Error handling here?
            if (store.name === name) continue
            console.log('REPLICATING ANOTHER')
            store._inner.replicate({ ...replicationOpts, stream: outerStream || stream })
          }
        })
        .catch(err => stream.destroy(err))

      return stream
    }
  }

  async info (opts) {

  }

  async seed (idx) {
    console.error('MEGASTORE CALLING SEED FOR IDX:', idx)
    if (idx instanceof Buffer) idx = datEncoding.encode(idx)

    const record = await this._storeIndex.get(CORESTORE_PREFIX + idx)
    const dkey = datEncoding.decode(record.discoveryKey)

    if (!this.networking || this.isSeeding(record.discoveryKey)) return
    record.opts.seed = true

    await this._storeIndex.batch([
      { type: 'put', key: CORESTORE_PREFIX + idx, value: record },
      { type: 'put', key: CORESTORE_PREFIX + record.discoveryKey, value: record }
    ])

    this._seed(record.discoveryKey)

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

    this._unseed(record.discoveryKey)

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
