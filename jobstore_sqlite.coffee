{JobInfo, JobStore, Blob, FutureResult} = require './index'
sqlite3 = require 'sqlite3'

jobCols = [
  'hash'
  'name'
  'description'
  'state'
  'submittedBy'
  'resourceInfo'
  'fromCache'
  'pure'
  'submitTime'
  'startTime'
  'endTime'
  'logBlob'
]

module.exports = class JobStoreSQLite extends JobStore
  constructor: (path=':memory:') ->
    @db = new sqlite3.Database(path)

  init: (cb) ->
    @db.serialize()
    @db.get "PRAGMA user_version;", (err, row) =>
      if parseInt(row.user_version, 10) < 2
        @createSchema(cb)
      else cb()

  createSchema: (cb) ->
    console.log "Initializing database"
    @db.serialize =>
      @db.run "CREATE TABLE jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        #{jobCols.join(',')}
      );"
      @db.run "CREATE UNIQUE INDEX jobs_hash ON jobs (hash);"
      @db.run "CREATE TABLE jobs_closure (
        parent,
        child,
        depth
      );"
      @db.run "CREATE INDEX jobs_closure_parent on jobs_closure (parent);"
      @db.run "CREATE INDEX jobs_closure_child on jobs_closure (child);"
      @db.run "CREATE TRIGGER jobs_closure_selfref AFTER INSERT ON jobs
                FOR EACH ROW BEGIN
                 INSERT INTO jobs_closure (parent, child, depth) VALUES (new.id, new.id, 0);
                END;"
      @db.run "CREATE TABLE inputs (
        jobId,
        name,
        type,
        value,
        UNIQUE(jobId, name)
      );"
      @db.run "CREATE INDEX inputs_jobId ON inputs (jobId);"
      @db.run "CREATE TABLE results (
        jobId,
        name,
        type,
        value,
        UNIQUE(jobId, name)
      );"
      @db.run "CREATE INDEX results_jobId ON results (jobId);"
      @db.run "PRAGMA user_version=2;", -> cb()

  addJob: (job, cb) ->
    @db.run "INSERT INTO jobs (#{jobCols.join(',')}) VALUES (#{('?' for i in jobCols).join(',')});",
      (job[k] for k in jobCols), (err) ->
        if err
          throw err
        job.id = this.lastID
        cb(job)

    @listen job

  listen: (job) ->
    job.on 'state', =>
      @updateJob(job)

    job.on 'dependencyAdded', (dependency) =>
      @addDependency(job, dependency)

    job.on 'inputsReady', =>
      @addInputs(job)

    job.on 'computed', =>
      @addResults(job)

  jobFromRow: (row, inputs, results) ->
    return null unless row
    j = new JobInfo()
    j.id = row.id
    for k in jobCols
      j[k] = row[k]

    if inputs?
      j.inputs = {}
      for r in inputs
        j.inputs[r.name] = @deserializeValue(r.type, r.value)

    if results?
      j.results = {}
      for r in results
        j.results[r.name] = @deserializeValue(r.type, r.value)

    j

  getJob: (id, cb) ->
    @db.get "SELECT id, #{jobCols.join(',')} FROM jobs WHERE id=?;", [id], (err, row) =>
      if err
        console.error(err)
        cb(null)
      @db.all "SELECT name, type, value FROM inputs WHERE jobId=?;", [id], (err, inputs) =>
        console.error(err) if err
        @db.all "SELECT name, type, value FROM results WHERE jobId=?;", [id], (err, results) =>
          console.error(err) if err
          cb(@jobFromRow(row, inputs, results))

  updateJob: (job) ->
    @db.run "UPDATE jobs SET state=?, hash=?, fromCache=?, startTime=?, endTime=?, logBlob=? WHERE id=?",
                         [job.state, job._hash, job.fromCache, job.startTime, job.endTime,
                         job.logBlob?.id, job.id]

  addDependency: (job, dep) ->
    @db.run "INSERT INTO jobs_closure (parent, child, depth)
               SELECT p.parent, c.child, p.depth+c.depth+1
               FROM jobs_closure p, jobs_closure c
               WHERE p.child=? AND c.parent=?", [job.id, dep.id]

  getRelatedJobs: (id, cb) ->
    @db.all "SELECT id, #{jobCols.join(',')} FROM jobs WHERE id IN (
              SELECT child  FROM jobs_closure WHERE parent=? UNION ALL
              SELECT parent FROM jobs_closure WHERE child=?);", [id, id], (err, rows) =>
      console.error(err) if err
      return cb(null) unless rows
      cb(@jobFromRow(row) for row in rows)

  addInputs: (job) ->
    @db.serialize =>
      for key, v of job.inputs
        [type, value] = @serializeValue(v)
        @db.run "INSERT INTO inputs (jobId, name, type, value) VALUES (?,?,?,?)", [job.id, key, type, value]

  addResults: (job) ->
    @db.serialize =>
      for key, v of job.results
        [type, value] = @serializeValue(v)
        @db.run "INSERT INTO results (jobId, name, type, value) VALUES (?,?,?,?)", [job.id, key, type, value]

  serializeValue: (v) ->
    if v instanceof FutureResult
      v = v.get()
    if v instanceof Blob
      ['blob', v.id]
    else
      ['json', JSON.stringify(v)]

  deserializeValue: (type, value) ->
    switch type
      when 'json' then JSON.parse(value)
      when 'blob' then new Blob(@blobStore, value)
