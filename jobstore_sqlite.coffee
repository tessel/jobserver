{JobInfo, JobStore} = require './index'
sqlite3 = require 'sqlite3'

jobCols = [
  'hash'
  'name'
  'description'
  'state'
  'pure'
  'parentJob'
  'submitTime'
  'startTime'
  'endTime'
  'logBlob'
]

module.exports = class JobStoreSQLite extends JobStore
  constructor: (path=':memory:') ->
    @db = new sqlite3.Database(path)
    @createSchema()

  createSchema: ->
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
        value,
        fromJob
      );"
      @db.run "CREATE INDEX inputs_jobId ON inputs (jobId);"
      @db.run "CREATE TABLE outputs (
        jobId,
        name,
        value
      );"
      @db.run "CREATE INDEX outputs_jobId ON outputs (jobId);"

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
  jobFromRow: (row) ->
    return null unless row
    j = new JobInfo()
    j.id = row.id
    for k in jobCols
      j[k] = row[k]
    j
      
  getJob: (id, cb) ->
    @db.get "SELECT id, #{jobCols.join(',')} FROM jobs WHERE id=?;", [id], (err, row) =>
      if err
        console.error(err)
      cb(@jobFromRow(row))

  updateJob: (job) ->
    @db.run "UPDATE jobs SET state=?, startTime=?,   endTime=?,  logBlob=? WHERE id=?",
                         [job.state, job.startTime, job.endTime,
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
      
