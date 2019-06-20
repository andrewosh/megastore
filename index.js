const { EventEmitter } = require('events')
const datEncoding = require('dat-encoding')
const sub = require('subleveldown')
const crypto = require('hypercore-crypto')
const corestore = require('random-access-corestore')
const collect = require('stream-collector')

const CORESTORE_PREFIX = 'corestore/'
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
        console.error(this._id, 'ATTEMPING TO REPLICATE:', dkey)
        const existing = this._corestoresByDKey.get(dkey)
        console.log('EXISTING?', !!existing)
        if (existing) return existing.replicate.bind(existing)
        try {
          const { name, key, opts: coreOpts } = await this._storeIndex.get('corestore/' + dkey)
          console.error('NAME:', name, 'KEY:', key, 'opts:', coreOpts)
          if (coreOpts.seed === false) return null

          console.error('ACTUALLY REPLICATING')
          console.error('IS IT CACHED?', !!this._corestoresByDKey.get(dkey))

          const store = this._corestoresByDKey.get(dkey) || this.get(name)

          // Inflating the default hypercore here will set the default key and bootstrap replication.
          store.default(datEncoding.decode(key))

          console.error('GOT A STORE')

          return store.replicate.bind(store)
        } catch (err) {
          if (!err.notFound) throw err
          console.log('REPLICATOR NOT FOUND')
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

  _loadKeys () {
    return new Promise((resolve, reject) => {
      collect(this._keyIndex.createReadStream(), (err, list) => {
        if (err) return reject(err)
        const mapList = list.map(({ key, value }) => [key, {
          publicKey: Buffer.from(value.publicKey, 'hex'),
          secretKey: Buffer.from(value.secretKey, 'hex')
        }])
        this._keys = new Map(mapList)
        return resolve()
      })
    })
  }

  _reseed () {
    return new Promise(async resolve => {
      const stream = this._storeIndex.createReadStream({
        gt: CORESTORE_PREFIX,
        lt: CORESTORE_PREFIX + String.fromCharCode(65535)
      })
      stream.on('data', ({ key, value }) => {
        if (value.seed === false) return
        key = key.slice(10)
        try {
          var decodedKey = datEncoding.decode(key)
        } catch (err) {
          return
        }
        console.error('RESEEDING KEY ON RESTART:', key)
        this._seeding.add(key)
        this.networking.seed(decodedKey)
      })
      stream.on('end', resolve)
      stream.on('error', err => reject(err))
    })
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

    console.error('GETTING CORESTORE WITH OPTS:', opts)
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

      console.error('PROCESSING CORE WITH OPTS:', opts, 'COREOPTS:', coreOpts)

      const value = {
        key: core.key,
        seed: opts.seed !== false,
        writable: core.writable,
      }

      batch.push({ type: 'put', key: CORE_PREFIX + encodedDiscoveryKey, value })
      self._cores.set(encodedKey, { core, refs: [name] })

      if (coreOpts.default) {
        mainDiscoveryKeyString = encodedDiscoveryKey
        mainKeyString = encodedKey

        if (self.networking && opts.seed !== false) {
          self.networking.seed(core.discoveryKey)
          self._seeding.add(mainDiscoveryKeyString)
        }

        self._corestores.set(name, wrappedStore)
        self._corestores.set(mainKeyString, wrappedStore)
        self._corestoresByDKey.set(mainDiscoveryKeyString, wrappedStore)

        const record = { name, opts: { ...opts, ...coreOpts }, key: encodedKey, discoveryKey: encodedDiscoveryKey }

        batch.push({ type: 'put', key: CORESTORE_PREFIX + encodedDiscoveryKey, value: record })
        batch.push({ type: 'put', key: CORESTORE_PREFIX + name, value: record })
      } else {
        batch.push({ type: 'put', key: SUBCORE_PREFIX +  mainDiscoveryKeyString + '/' + encodedDiscoveryKey, value: {}})
      }

      self._storeIndex.batch(batch, err => {
        if (err) this.emit('error', err)
      })
    }

    function close (cb) {
      if (self.networking && mainDiscoveryKeyString && self.isSeeding(mainDiscoveryKeyString)) {
        self.networking.unseed(mainDiscoveryKeyString)
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
    console.error('MEGASTORE SEEDING IDX:', idx)

    const record = await this._storeIndex.get(CORESTORE_PREFIX + idx)
    const dkey = datEncoding.decode(record.discoveryKey)
    console.error('RECORD:', record, 'DKEY:', dkey)

    if (!this.networking || this.isSeeding(record.discoveryKey)) return
    record.opts.seed = true

    await this._storeIndex.batch([
      { type: 'put', key: CORESTORE_PREFIX + idx, value: record },
      { type: 'put', key: CORESTORE_PREFIX + record.discoveryKey, value: record }
    ])

    this._seeding.add(record.discoveryKey)
    this.networking.seed(dkey)
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
