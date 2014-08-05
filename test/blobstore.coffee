assert = require 'assert'
jobserver = require '../index'
BlobStoreLocal = require '../blobstore_local'
temp = require 'temp'
temp.track()

testBlobStore = (constr) ->
  it 'stores and retrieves blobs', (next) ->
    s = constr()
    data = new Buffer('asdfghjkl')
    b1 = s.putBlob(data)
    s.getBlob b1.id, (resultData) ->
      assert.equal(data.toString('hex'), resultData.toString('hex'))
      next()

  it 'hashes identical blobs to the same id', ->
    s = constr()
    data = new Buffer('asdfghjkl')
    b1 = s.putBlob(data)
    b2 = s.putBlob(data)
    assert.equal(b1.id, b2.id)

describe 'BlobStoreMem', ->
  testBlobStore -> new jobserver.BlobStoreMem()

temp.mkdir "jobserver-test", (err, dir) =>
  describe 'BlobStoreLocal', ->
    testBlobStore -> new BlobStoreLocal(dir)
