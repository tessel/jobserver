assert = require 'assert'
jobserver = require '../jobserver'

describe 'blobStoreMem', ->
	it 'stores and retrieves blobs', (next) ->
		s = new jobserver.BlobStoreMem()
		data = new Buffer('asdfghjkl')
		b1 = s.putBlob(data)
		s.getBlob b1.id, (b2) ->
			assert.equal(b1.data, b2.data)
			next()

	it 'hashes identical blobs to the same id', ->
		s = new jobserver.BlobStoreMem()
		data = new Buffer('asdfghjkl')
		b1 = s.putBlob(data)
		b2 = s.putBlob(data)
		assert.equal(b1.id, b2.id)
		assert.equal(b1.data, b2.data)

# Takes a list of names and returns an object with methods with those names, which must be called in order
orderingSpy = (events) ->
	r = {}
	counter = 0

	events.forEach (name) ->
		r[name] = ->
			if events[counter] == name
				counter++
			else
				throw new Error("`#{name}` called in the wrong order (expected `#{events[counter]}` next)")

	return r

describe 'orderingSpy', ->
	it 'passes', ->
		o = orderingSpy(['a', 'b', 'c', 'c', 'd'])
		o.a()
		o.b()
		o.c()
		o.c()
		o.d()

	it 'fails', ->
		o = orderingSpy(['a', 'b', 'c', 'd'])
		assert.throws ->
			o.a()
			o.b()
			o.d()
			o.c()

class TestJob extends jobserver.Job
	constructor: (@doExec) ->
		super()

describe 'Job', ->
	jobstore = blobstore = server = null

	beforeEach ->
		jobstore = new jobserver.JobStoreMem()
		blobstore = new jobserver.BlobStoreMem()
		server = new jobserver.Server(jobstore, blobstore)

	it 'Runs and emits states and collects a log', (cb) ->
		ordered = orderingSpy(['waiting', 'running', 'exec', 'success', 'settled'])
		job = new TestJob (cb) ->
			ordered.exec()
			@logStream.write("test1\n")
			setImmediate =>
				@logStream.write("test2\n")
				cb(true)

		job.on 'state', (s) ->
			ordered[s]()

		job.on 'settled', ->
			ordered.settled()
			assert.equal(@logStream.log, 'test1\ntest2\n')
			cb()

		server.submit(job)

	it 'Runs dependencies in order', (done) ->
		ordered = orderingSpy(['prestart', 'depstart', 'depstart', 'depdone', 'depdone' , 'parentstart'])

		job0 = new TestJob (cb) ->
			ordered.prestart()
			setImmediate(-> cb(true))

		depfn = (cb) ->
			ordered.depstart()
			setImmediate ->
				ordered.depdone()
				cb(true)

		job1 = new TestJob depfn
		job2 = new TestJob depfn
		job3 = new TestJob (cb) ->
			ordered.parentstart()
			setImmediate(-> cb(true))

		job1.explicitDependencies.push(job0)
		job2.explicitDependencies.push(job0)
		job3.explicitDependencies.push(job1, job2)

		server.submit job3, ->
			done()

	it 'Generates implicit dependencies based on input'
	it 'Rejects dependency cycles'
	it 'Hashes consistently'
	it 'Avoids recomputing calculated jobs'


