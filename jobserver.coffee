{EventEmitter} = require('events')
{Transform} = require('stream')
crypto = require('crypto')

BLOB_HMAC_KEY = 'adb97d77011182f0b6884f7a6d32280a'
JOB_HMAC_KEY  = 'fb49246c50d78d460a5d666e943230fa'

STATES = [
	'waiting'   # depends on outputs of other jobs that have not finished yet
	'pending'   # ready to run, but blocked on hardware resources
	'running'   # self-explanatory
	'success'   # Ran and produced its outputs
	'fail'      # Ran and failed to produce its outputs. This status is cached. Another run with the same inputs would also fail.
	'abort'     # Ran and did not produce its output due to e.g. a network problem. Running again may succeed.
]


counter = 0
@Server = class Server extends EventEmitter
	constructor: (@jobStore, @blobStore) ->
		@activeJobs = {}

	makeJobId: -> (counter++)

	submit: (job, doneCb) ->
		server = this
		job.id = @makeJobId()
		@activeJobs[job.id] = job

		server.emit 'submit', job

		job.on 'state', (state) ->
			server.emit 'job.state', this, state

		job.once 'settled', =>
			delete @activeJobs[job.id]
			doneCb() if doneCb

		job.submitted(this)

class FutureResult
	constructor: (@job, @key) ->
	get: ->
		if @job.status == 'success'
			@job.result[@key]
		else
			throw new Error("Accessing result of job with status #{@job.status}")

class BlobStore
	newBlob: (buffer, meta) ->
		throw new Error("Abstract method")
	getBlob: (id, cb) ->
		throw new Error("Abstract method")
	hash: (buffer) ->
		crypto.createHmac('sha256', BLOB_HMAC_KEY).update(buffer).digest().toString('base64')


class Blob
	constructor: (@store, @id, @meta, @cached) ->

class JobStore


class TeeStream extends Transform
	constructor: ->
		super()
		@log = ''

	_transform: (chunk, encoding, callback) ->
		@log += chunk.toString('utf8')
		this.push(chunk)
		callback()

@Job = class Job extends EventEmitter
	constructor: (@executor=false, @pure=false, @inputs={}, @explicitDependencies=[], resultNames=[]) ->
		@state = null
		@result = {}
		for key in resultNames
			@result[key] = new FutureResult(this, key)

	submitted: (@server) ->
		@dependencies = @explicitDependencies.slice(0)
		for i in @inputs
			if i instanceof FutureResult
				@dependencies.push(i.job)

		for dep in @dependencies
			if not dep.state?
				server.submit(dep)

			unless dep.settled()
				dep.once 'settled', =>
					@checkDeps()

		@saveState 'waiting'
		@checkDeps()

	checkDeps: ->
		unless @state is 'waiting'
			throw new Error("checkDeps in state #{@state}")

		ready = true
		for dep in @dependencies
			switch dep.state
				when 'error'
					return @saveState 'error'
				when 'abort'
					return @saveState 'abort'
				when 'success'
					# nothing
				else
					ready = false

		if ready
			if @pure
				@server.jobStore.resultByHash @hash(), (completion) =>
					if result
						@result.fromCache = completion.id or true
						{@result, @startTime, @endTime} = completion
						@saveState(completion.status)
					else
						@enqueue()
			else
				@enqueue()

	hash: ->
		unless @pure
			throw new Error("Can't hash impure job (pure jobs cannot depend on impure jobs)")

		unless @_hash
			hasher = crypto.createHmac('sha256', JOB_HMAC_KEY)
			hasher.update(@name)

			depHashes = (dep.hash() for dep in @explicitDependencies)
			depHashes.sort()
			hasher.update(hash) for hash in depHashes

			for key in Object.keys(@inputs).sort()
				hasher.update(key)
				hasher.update(":")
				value = @inputs[key]
				if value instanceof FutureResult
					value = value.get()

				if value instanceof Blob
					hasher.update(value.hash)
				else
					hasher.update(JSON.stringify(value))
				hasher.update(",")

			@_hash = hasher.digest()

		@_hash

	enqueue: ->
		if @executor
			@saveState 'pending'
			@executor.enqueue(this)
		else
			@exec()

	settled: ->
		@state in ['success', 'fail', 'abort']

	saveState: (state) ->
		if state not in STATES
			throw new Error("Invalid status '#{state}'")
		@state = state
		@emit 'state', state

		if @settled()
			@emit 'settled'

	exec: ->
		@startTime = new Date()
		@saveState 'running'
		@logStream = new TeeStream()
		@doExec (result) =>
			@endTime = new Date()
			@fromCache = false

			if @pure
				@emit 'computed'

			if result
				@result = result
				@saveState('success')
			else
				@saveState('fail')
	
	name: ''
	description: ''

	# Override this
	doExec: (cb) ->
		@logStream.write("Default exec!")
		setImmediate( -> cb(false) )

@BlobStoreMem = class BlobStoreMem extends BlobStore
	constructor: ->
		@blobs = {}

	putBlob: (buffer, meta) ->
		id = @hash(buffer)
		if not @blobs[id]
			@blobs[id] = new Blob(this, id, meta, buffer)
		@blobs[id]

	getBlob: (id, cb) ->
		v = @blobs[id]
		setImmediate ->
			cb(v)
		return

@JobStoreMem = class JobStoreMem extends JobStore
	constructor: ->

