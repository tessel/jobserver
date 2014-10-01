assert = require 'assert'
jobserver = require '../index'

class TestJob extends jobserver.Job
  name: 'test'
  description: 'Test Job'
  constructor: (@run) ->
    super()

class OrderingJob extends jobserver.Job
  counter = 1
  run: (ctx, cb) ->
    this.startTick = counter++
    setTimeout (=>
      this.endTick = counter++
      cb()
    ), 1

  assertRanAfter: (job) ->
    assert job.startTick and job.endTick, "other job did not run"
    assert @startTick and @endTick, "this job did not run"
    assert job.endTick < @startTick

describe 'Job', ->
  jobstore = blobstore = server = null

  before (done) ->
    server = new jobserver.Server()
    server.defaultResource = new jobserver.Resource()
    server.init done

  describe 'Running a job', (cb) ->
    job = null
    order = []

    before (done) ->
      job = new TestJob (ctx) ->
        order.push('exec')
        ctx.write("test1\n")
        ctx.write("test2\n")
        null

      job.on 'state', (s) ->
        order.push(s)

      job.on 'settled', done
      server.submit(job)

    it 'emits states', ->
      assert.deepEqual order, ['waiting', 'pending', 'running', 'exec', 'success']

    it 'collects a log', ->
      assert.equal(job.log, 'test1\ntest2\n')

  describe 'Running dependent jobs', (done) ->
    jobs = []
    before (done) ->
      makeJob = (deps...) ->
        j = new OrderingJob()
        j.explicitDependencies.push(deps...)
        j

      #          1
      #  5 - 3 <   > 0
      #   \      2
      #    4

      jobs[0] = makeJob()
      jobs[1] = makeJob(jobs[0])
      jobs[2] = makeJob(jobs[0])
      jobs[3] = makeJob(jobs[1], jobs[2])
      jobs[4] = makeJob()
      jobs[5] = makeJob(jobs[4], jobs[3])

      server.submit jobs[5], ->
        done()

    it 'runs dependencies before dependants', ->
      jobs[1].assertRanAfter(jobs[0])
      jobs[2].assertRanAfter(jobs[0])
      jobs[3].assertRanAfter(jobs[1])
      jobs[3].assertRanAfter(jobs[2])
      jobs[5].assertRanAfter(jobs[3])
      jobs[5].assertRanAfter(jobs[4])

    checkRelated = (id, relatedIds, cb) ->
      server.relatedJobs jobs[id].id, (l) ->
        assert.deepEqual (i.id for i in l).sort(), (jobs[i].id for i in relatedIds).sort()
        cb()

    it 'persists children of root jobs to the database', (done) ->
      checkRelated 5, [0, 1, 2, 3, 4, 5], done

    it 'persists parents of child jobs to the database', (done) ->
      checkRelated 0, [0, 1, 2, 3, 5], done

    it 'persists children and parents of middle jobs to the database', (done) ->
      checkRelated 2, [0, 2, 3, 5], done

    describe 'contains failure', ->
      it 'at the top level', (done) ->
        job = new TestJob (ctx) ->
          throw new Error("Test error")
        server.submit job, ->
          assert.equal job.state, 'fail'
          done()

      it 'in callbacks', (done) ->
        job = new TestJob (ctx) ->
          (cb, ctx) ->
            setImmediate -> throw new Error("Test error")
        server.submit job, ->
          assert.equal job.state, 'fail'
          done()

  it 'persists inputs and results', (done) ->
    j1 = new TestJob [
      (ctx, cb) ->
        @results['outStr'] = 'test'
        cb()
      (ctx, cb) ->
        data = new Buffer('Hello World')
        @results['outBlob'] = @server.blobStore.putBlob(data, {}, cb)
    ]
    j1.inputs.inStr = 'foo'
    j1.results.outStr = new jobserver.FutureResult(j1, 'outStr')
    j1.results.outBlob = new jobserver.FutureResult(j1, 'outBlob')

    j2 = new TestJob (ctx) ->
      @results.outBlob2 = @inputs.inBlob
      null
    j2.inputs.inBlob = j1.results.outBlob

    server.submit j2, ->
      assert.equal j1.state, 'success'
      assert.equal j2.state, 'success'

      server.job j1.id, (j1db) =>
        server.job j2.id, (j2db) =>
          assert.equal j1db.constructor.name, 'JobInfo' # these should be the deserialized versions
          assert.equal j2db.constructor.name, 'JobInfo'

          assert.equal j1db.name, 'test'
          assert.equal j1db.state, 'success'
          assert.equal j1db.description, TestJob::description

          assert.equal j1db.inputs.inStr, 'foo'
          assert.equal j1db.results.outStr, 'test'
          assert j1db.results.outBlob instanceof jobserver.Blob
          h = jobserver.BlobStore::hash('Hello World')
          assert.equal j1db.results.outBlob.id, h
          assert.equal j2db.inputs.inBlob.id, h
          assert.equal j2db.results.outBlob2.id, h

          done()

  it 'aborts if dependencies fail', (done) ->
    j1 = new TestJob (ctx, cb) ->
      cb("testErr")
    j1.results.test = new jobserver.FutureResult(j1, 'test')
    j3 = new TestJob (ctx, cb) ->
      setTimeout(cb, 1)
    j2 = new TestJob (ctx) ->
    j2.inputs.test = j1.results.test
    j2.explicitDependencies.push(j1)
    j2.explicitDependencies.push(j3)
    server.submit j2, ->
      assert.equal j1.state, 'fail'
      assert.equal j2.state, 'abort'
      done()

  it 'Can run a job as a step of another', (done) ->
    order = []
    j1 = new TestJob (ctx) -> [
      -> order.push 'before'; null
      ctx.runJob new TestJob (ctx) ->
        order.push 'sub'; null
      -> order.push 'after'; null
    ]
    server.submit j1, ->
      assert.equal j1.state, 'success'
      assert.deepEqual order, ['before', 'sub', 'after']

      server.relatedJobs j1.id, (l) ->
        assert.equal l.length, 2
        done()

  it 'Fails if a child job fails', (done) ->
    j1 = new TestJob (ctx) ->
      ctx.runJob new TestJob (ctx) ->
        throw new Error("test job")
    server.submit j1, ->
      assert.equal j1.state, 'fail'
      done()

  it 'Generates implicit dependencies based on input'
  it 'Rejects dependency cycles'
  it 'Hashes consistently'
  it 'Avoids recomputing calculated jobs'

describe 'SeriesResource', ->
  server = null
  before (done) ->
    server = new jobserver.Server()
    server.defaultResource = new jobserver.Resource()
    server.init done

  e = null
  beforeEach ->
    e = new jobserver.SeriesResource(new jobserver.Resource())

  it 'Runs jobs in order', (done) ->
    jobs = (new OrderingJob(e) for i in [0...3])
    server.submit(j) for j in jobs
    jobs[jobs.length-1].on 'settled', ->
      for i in [1...3]
        jobs[i].assertRanAfter(jobs[i-1])
      done()

describe 'LocalResource', ->
    server = null
    e = null
    blobstore = null
    beforeEach (done) ->
      server = new jobserver.Server()
      e = server.defaultResource = new jobserver.LocalResource()
      server.init done

    it 'Runs subtasks in sequence', (cb) ->
      order = []
      j = new TestJob (ctx) -> [
        ->
          order.push 'a'
          null
        (c) ->
          assert.equal(ctx, c)
          order.push('b')
          null
        (c, next) ->
          assert.equal(ctx, c)
          order.push('c')
          setTimeout(next, 1)
        -> [
          ->
            order.push 'd'
            null
        ]
      ]
      server.submit j, ->
        assert.deepEqual order, ['a', 'b', 'c', 'd']
        cb()

    it 'Runs commands', (cb) ->
      j = new TestJob (ctx) ->
        ctx.run('touch test.txt')
      server.submit j, ->
        assert.equal(j.state, 'success')
        cb()

    it 'Fails if commands fail', (cb) ->
      j = new TestJob (ctx) ->
        ctx.run('false')
      server.submit j, ->
        assert.equal(j.state, 'fail')
        cb()

    it 'Saves files', (cb) ->
      j = new TestJob (ctx) -> [
        ctx.run('echo hello > test.txt')
        ctx.get('test', 'test.txt')
      ]
      server.submit j, ->
        assert.equal(j.state, 'success')
        j.results.test.getBuffer (data) ->
          assert.equal(data.toString('utf8'), 'hello\n')
          cb()

    it 'Loads files', (cb) ->
      b = server.blobStore.putBlob(new Buffer("Hello\n"))
      j = new TestJob (ctx) -> [
        ctx.put(@inputs.test, 'test.txt')
        ctx.run 'echo Hello > test2.txt'
        ctx.run 'diff -u test.txt test2.txt'
      ]
      j.resource = e
      j.inputs.test = b
      server.submit j, ->
        assert.equal(j.state, 'success')
        cb()
