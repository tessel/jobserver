{EventEmitter} = require('events')
{Transform} = require('stream')
crypto = require('crypto')

util = require('util')
temp = require('temp')
rimraf = require('rimraf')
child_process = require('child_process')
path = require 'path'
fs = require 'fs'

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

# A `Server` maintains the global job list and aggregates events for the UI
@Server = class Server extends EventEmitter
	constructor: (@jobStore, @blobStore) ->
		@activeJobs = {}

	counter = 0
	makeJobId: -> (counter++)

	submit: (job, doneCb) ->
		server = this
		job.id = @makeJobId()
		@activeJobs[job.id] = job

		server.emit 'submit', job

		job.on 'state', (state) ->
			server.emit 'job.state', this, state

		job.once 'settled', =>
			#delete @activeJobs[job.id]
			doneCb() if doneCb

		job.submitted(this)
		
	job: (id) ->
		@activeJobs[id]

	jsonableState: ->
		jobs = for id, job of @activeJobs when not job.settled()
			job.jsonableState()

		{jobs}

# A `FutureResult` is a reference to a result of a `Job` which may not yet have completed
@FutureResult = class FutureResult
	constructor: (@job, @key) ->
	get: ->
		if @job.state == 'success'
			@job.result[@key]
		else
			throw new Error("Accessing result of job with status #{@job.status}")
			
	getBuffer: (cb) -> @get().getBuffer(cb)
	getId: -> @get().id

# `BlobStore` is the abstract base class for result file data storage.
# Subclasses persist Buffers and retrieve them by hash.
@BlobStore = class BlobStore
	newBlob: (buffer, meta) ->
		throw new Error("Abstract method")
	getBlob: (id, cb) ->
		throw new Error("Abstract method")
	hash: (buffer) ->
		crypto.createHmac('sha256', BLOB_HMAC_KEY).update(buffer).digest().toString('base64')

# An item in a BlobStore
class Blob
	constructor: (@store, @id, @meta) ->
		
	getBuffer: (cb) -> @store.getBlob(@id, cb)
	getId: -> @id

# Abstact base class for database of job history
@JobStore = class JobStore

# A Stream transformer that captures a copy of the streamed data and passes it through
class TeeStream extends Transform
	constructor: ->
		super()
		@log = ''
		
	pipeAll: (o) ->
		o.write(@log)
		@pipe(o)

	_transform: (chunk, encoding, callback) ->
		@log += chunk.toString('utf8')
		this.push(chunk)
		callback()

# Object containing the state and logic for a job. Subclasses can override the behavior
@Job = class Job extends EventEmitter
	resultNames: []
	
	constructor: (@executor, @inputs={}) ->
		@pure = false
		@explicitDependencies = []
		@state = null
		@result = {}
	
		for key in @resultNames
			@result[key] = @result[key] = new FutureResult(this, key)
			
		@config()
			
	config: ->

	jsonableState: ->
		{@id, @name, @description, @state, settled: @settled()}

	submitted: (@server) ->
		@executor ?= @server.defaultExecutor
		
		@dependencies = @explicitDependencies.slice(0)
		for k, v of @inputs
			if v instanceof FutureResult
				@dependencies.push(v.job)

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
		@saveState 'pending'
		@executor.enqueue(this)

	settled: ->
		@state in ['success', 'fail', 'abort']

	saveState: (state) ->
		if state not in STATES
			throw new Error("Invalid status '#{state}'")
		@state = state
		@emit 'state', state

		if @settled()
			@emit 'settled'

	beforeRun: (@ctx) ->
		@startTime = new Date()
		@saveState 'running'
		
	afterRun: (result) ->
		@endTime = new Date()
		@fromCache = false

		if @pure
			@emit 'computed'

		if result
			@saveState('success')
		else
			@saveState('fail')

	name: ''
	description: ''

	# Override this
	run: (ctx) ->
		ctx.write("Default exec!\n")
		setImmediate( -> ctx.done(null) )

# An in-memory BlobStore
@BlobStoreMem = class BlobStoreMem extends BlobStore
	constructor: ->
		@blobs = {}

	putBlob: (buffer, meta) ->
		id = @hash(buffer)
		if not @blobs[id]
			@blobs[id] = buffer
		new Blob(this, id, meta)

	getBlob: (id, cb) ->
		v = @blobs[id]
		setImmediate ->
			cb(v)
		return

@JobStoreMem = class JobStoreMem extends JobStore
	constructor: ->

# An Executor manages the execution of a set of jobs. May also wrap access to an execution resource
@Executor = class Executor
	enqueue: (job) ->
		ctx = @makeContext(job)
		ctx.before (err) ->
			throw err if err
			try
				job.beforeRun(ctx)
				job.run(ctx)
			catch e
				ctx._done(e)
	
	makeContext: (job) ->
		new this.Context(job)
	
	# An Executor provides a Job a Context to access resources
	Context: class Context extends TeeStream
		constructor: (@job)->
			super()
			@_completed = false
			@queue = []
			# Note: this needs to be piped somewhere by default so the Transform doesn't accumulate data.
			# If not stdout, then a null sink, or some other way of fixing this.
			@pipe(process.stdout)
			
		before: (cb) ->
			setImmediate(cb)
		
		after: (cb) ->
			setImmediate(cb)
			
		done: (result) ->
			unless @queue.length
				@_done(result)
			
		_done: (err) ->
			if err
				@write("Failed with error: #{err.stack ? err}\n")
				
			if @_completed
				console.trace("Job #{@job.constructor.name} completed multiple times")
				return
			@_completed = true
				
			@after (e) =>
				throw e if e
				@end()
				@job.log = @log
				@job.afterRun(!err)
			
		then: (fn) ->
			next = (err) =>
				@queue.shift()
				if err
					return @_done(err)
				
				if @queue.length
					f = @queue[0]
					setImmediate => f.call(this, next)
				else
					@_done()
				
				return null
				
			if @queue.push(fn) == 1
				setImmediate => fn.call(this, next)
				

# An executor combinator that runs jobs one at a time in series on a specified executor
@SeriesExecutor = class SeriesExecutor extends Executor
	constructor: (@executor) ->
		super()
		@currentJob = null
		@queue = []

	enqueue: (job) =>
		@queue.push(job)
		@shift() unless @currentJob

	shift: =>
			@currentJob = @queue.shift()
			if @currentJob
				@currentJob.on 'settled', @shift
				@executor.enqueue(@currentJob)

@LocalExecutor = class LocalExecutor extends Executor
	Context: class Context extends Executor::Context
		before: (cb) ->
			temp.mkdir "jobserver-#{@job.name}", (err, @dir) =>
				@_env = {}
				@envImmediate(process.env)
				@_cwd = @dir
				@write("Working directory: #{@dir}\n")
				cb(err)

		after: (cb) -> 
			rimraf @dir, cb
			
		envImmediate: (e) ->
			for k, v of e
				@_env[k] = v

		env: (e) ->
			@then (cb) ->
				@envImmediate(e)
				cb()

		cd: (p) ->
			@then (cb) ->
				@_cwd = path.resolve(@_cwd, p)
				cb()

		run: (command, args) ->
			@then (cb) =>
				unless util.isArray(args)
					args = ['-c', command]
					command = 'sh'

				@write("$ #{command + if args then ' ' + args.join(' ') else ''}\n")
				p = child_process.spawn command, args, {cwd: @_cwd, env: @_env}
				p.stdout.pipe(this, {end: false})
				p.stderr.pipe(this, {end: false})
				p.on 'close', (code) =>
					cb(if code != 0 then "Exited with #{code}")
					
		put: (content, filename) ->
			@then (cb) =>
				content.getBuffer (data) =>
					console.log("#{data.length} bytes to #{path.resolve(@_cwd, filename)}")
					fs.writeFile path.resolve(@_cwd, filename), data, cb
			
		get: (output, filename) ->
			@then (cb) =>
				fs.readFile path.resolve(@_cwd, filename), (err, data) =>
					return cb(err) if err
					@job.result[output] = @job.server.blobStore.putBlob(data, {name: output})
					console.log("#{data.length} bytes from #{path.resolve(@_cwd, filename)} as #{output} on #{@job.id}:", @job.result[output])
					cb()
			
		git_clone: (repo, branch, dir) ->
			@run('git', ['clone', '--depth=1', '-b', branch, '--', repo, dir])

