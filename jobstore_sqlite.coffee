{JobInfo, JobStore, Blob, FutureResult} = require './index'
sqlite3 = require 'sqlite3'
{TransactionDatabase} = require 'sqlite3-transactions'
async = require 'async'

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

class JobStoreTransaction
  constructor: (@db) ->

  addJob: (job, cb) ->
    @db.run "INSERT INTO jobs (#{jobCols.join(',')}) VALUES (#{('?' for i in jobCols).join(',')});",
      (job[k] for k in jobCols), (err) ->
        if err
          throw err
        job.id = this.lastID
        cb(job)

  jobFromRow: (row, next) ->
    return next(null, null) unless row
    j = new JobInfo()
    j.id = row.id
    for k in jobCols
      j[k] = row[k]

    @db.all "SELECT name, type, value FROM inputs WHERE jobId=?;", [row.id], (err, inputs) =>
      return next(err) if err
      j.inputs = {}
      for r in inputs
        j.inputs[r.name] = @deserializeValue(r.type, r.value)

      @db.all "SELECT name, type, value FROM results WHERE jobId=?;", [row.id], (err, results) =>
        return next(err) if err
        j.results = {}
        for r in results
          j.results[r.name] = @deserializeValue(r.type, r.value)

        next(null, j)

  getJob: (id, cb) ->
    @db.get "SELECT id, #{jobCols.join(',')} FROM jobs WHERE id=?;", [id], (err, row) =>
      if err
        console.error(err)
        cb(null)
      @jobFromRow row, (err, j) ->
        console.error(err) if j
        cb(j)

  updateJob: (job, cb) ->
    @db.run "UPDATE jobs SET state=?, hash=?, fromCache=?, startTime=?, endTime=?, logBlob=? WHERE id=?",
                         [job.state, job._hash, job.fromCache, +job.startTime, +job.endTime,
                         job.logBlob?.id, job.id], cb

  addDependency: (job, dep, cb) ->
    @db.run "INSERT INTO jobs_closure (parent, child, depth)
               SELECT p.parent, c.child, p.depth+c.depth+1
               FROM jobs_closure p, jobs_closure c
               WHERE p.child=? AND c.parent=?", [job.id, dep.id], cb

  getRelatedJobs: (id, cb) ->
    @db.all "SELECT id, #{jobCols.join(',')} FROM jobs WHERE id IN (
              SELECT child  FROM jobs_closure WHERE parent=? UNION ALL
              SELECT parent FROM jobs_closure WHERE child=?);", [id, id], (err, rows) =>
      console.error(err) if err
      return cb(null) unless rows
      async.map rows, ((row, cb) => @jobFromRow(row, cb)), (err, jobs) ->
        console.error(err)
        cb(jobs)

  addInputs: (job, cb) ->
    eachKey = (key, next) =>
      [type, value] = @serializeValue(job.inputs[key])
      @db.run "INSERT INTO inputs (jobId, name, type, value) VALUES (?,?,?,?)", [job.id, key, type, value], next
    async.each Object.keys(job.inputs), eachKey, cb

  addResults: (job, cb) ->
    eachKey = (key, next) =>
      [type, value] = @serializeValue(job.results[key])
      @db.run "INSERT INTO results (jobId, name, type, value) VALUES (?,?,?,?)", [job.id, key, type, value], next
    async.each Object.keys(job.results), eachKey, cb

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

  commit: (cb) ->
    @db.commit(cb)


module.exports = class JobStoreSQLite extends JobStoreTransaction
  constructor: (path=':memory:') ->
    @db = new TransactionDatabase(new sqlite3.Database(path))

  transaction: (cb) ->
    @db.beginTransaction (err, transaction) ->
      throw err if err
      cb(new JobStoreTransaction(transaction))

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
