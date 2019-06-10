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

    if (this.networking) {
      this.networking.on('error', err => this.emit('error', err))
      this.networking.setReplicatorFactory(async dkey => {
        var store = this._corestoresByDKey.get(dkey)
        if (store) return store.replicate
        console.log('THERE IS NO CORESTORE CACHED')
        try {
          console.log('dkey:', dkey)
          const { name, key, opts } = await this._storeIndex.get('corestore/' + dkey)
          console.log('opts:', opts)
          if (opts.seed === false) return null
          console.log('NAME:', name, 'KEY:', key, 'OPTS:', opts)

          store = this.get(name)
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
    // Default feed keys are cached here so that they can be fetched synchronously. Created in _loadKeys.
    this._keys = null

    this.isReady = false
    this._ready = async () => {
      await this._loadKeys()
      await this.networking ? this._reseed() : Promise.resolve()
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
        gt: 'corestore/',
        lt: 'corestore/' + String.fromCharCode(65535)
      })
      stream.on('data', ({ key, value }) => {
        key = key.slice(10)
        const store = this.get(value.name, value.opts)
        this.networking.seed(datEncoding.decode(key))
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

      batch.push({ type: 'put', key: 'core/' + encodedDiscoveryKey, value })
      self._cores.set(encodedKey, { core, refs: [name] })

      if (coreOpts.default) {
        if (opts.seed !== false && self.networking) self.networking.seed(core.discoveryKey)

        mainDiscoveryKeyString = encodedDiscoveryKey
        mainKeyString = encodedKey

        self._corestores.set(mainKeyString, store)
        self._corestoresByDKey.set(mainDiscoveryKeyString, store)
        batch.push({ type: 'put', key: 'corestore/' + encodedDiscoveryKey, value: { name, opts, key: encodedKey } })
      } else {
        batch.push({ type: 'put', key: 'subcore/' +  mainDiscoveryKeyString + '/' + encodedDiscoveryKey, value: {}})
      }

      self._storeIndex.batch(batch, err => {
        if (err) this.emit('error', err)
      })
    }

    function close (cb) {
      if (self.networking && opts.seed !== false && mainDiscoveryKeyString) {
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

  async update (opts) {

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
