fs = require 'fs'
zlib = require 'zlib'
path = require 'path'
mkdirp = require 'mkdirp'
{BlobStore, Blob} = require './index'

module.exports = class BlobStoreLocal extends BlobStore
  constructor: (@localStorePath) ->
    mkdirp.sync(@localStorePath)

  path: (id) ->
    folder = path.join(@localStorePath, id.slice(0,2))
    mkdirp.sync(folder)
    path.join(folder, id)

  putBlob: (buffer, meta, cb) ->
    # writes buffer to local filesystem
    id = @hash(buffer)
    p = @path(id)

    fs.writeFile("#{p}.json", JSON.stringify(meta))
    zlib.gzip buffer, (err, b) ->
      throw err if err
      fs.writeFile("#{p}.gz", b, cb)

    new Blob(this, id, meta)


  getBlob: (id, cb) ->
    fs.readFile "#{@path(id)}.gz", (err, data) ->
      throw err if err
      zlib.gunzip data, (err, data) ->
        throw err if err
        cb(data)
