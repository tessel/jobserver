fs = require 'fs'
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

    fs.writeFile(@path(id)+'.json', JSON.stringify(meta))
    fs.writeFile(@path(id), buffer, cb)
    new Blob(this, id, meta)


  getBlob: (id, cb) ->
    fs.readFile @path(id), (err, data) ->
      throw err if err
      cb(data)
