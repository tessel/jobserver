assert = require 'assert'
jobserver = require '../index'
JobStoreSQLite = require '../jobstore_sqlite'

describe 'JobStore', ->
  jobstore = new JobStoreSQLite()
  job = new jobserver.Job()
  job.name = 'test_job'
  job.description = 'Test'
  job.state = 'pending'

  it 'stores jobs and adds an id', (done) ->
    jobstore.addJob job, ->
      assert job.id
      done()

  it 'retrieves a job', (done) ->
    jobstore.getJob job.id, (j) ->
      assert.equal j.id,           job.id
      assert.equal j.state,        job.state
      assert.equal j.description,  job.description
      done()

  it 'updates on state changes', (done) ->
    job.saveState('success')
    setTimeout (->
      jobstore.getJob job.id, (j) ->
        assert.equal j.state, 'success'
        done()
    ), 10
