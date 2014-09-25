assert = require 'assert'
jobserver = require '../index'
JobStoreSQLite = require '../jobstore_sqlite'

describe 'JobStore', ->
  jobstore = new JobStoreSQLite()
  job = new jobserver.Job()
  job.name = 'test_job'
  job.description = 'Test'
  job.state = 'pending'

  before (done) ->
    jobstore.init ->
      done()

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
    jobstore.transaction (t) ->
      job.state = 'success'
      t.updateJob job, ->
        t.commit ->
          setTimeout (->
            jobstore.getJob job.id, (j) ->
              assert.equal j.state, 'success'
              done()
          ), 10
