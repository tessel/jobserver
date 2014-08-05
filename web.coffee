express = require('express')
serverEvent = require('server-event')()
{Transform} = require('stream')

class SSEStream extends Transform
  _transform: (chunk, encoding, callback) ->
    s = 'data: ' + JSON.stringify(chunk.toString('utf8')) + '\r\n\r\n'
    this.push(s)
    callback()

  _flush: (callback) ->
    this.push("event: end\ndata: null\n\n")
    callback()

module.exports = web = (server) ->
  app = express()
  app.use(express.static(__dirname + '/ui'));
  index_page = __dirname + '/ui/index.html'

  connections = []

  # Subscribe a stream to job updates
  subscribe = (req, res, jobs) ->
    serverEvent(req, res)
    initialJobs = jobs ? (job for id, job of server.activeJobs)

    res.sse 'hello',
      server: server.jsonableState()
      jobs: (job.jsonableState() for job in initialJobs)

    res.jobs = if jobs?
      job.id for job in jobs when not job.settled()
    else null

    connections.push(res)

    res.on 'close', ->
      i = connections.indexOf(res)
      connections.splice(i, 1) if i >= 0
      console.log 'closed response:', connections.length, 'left'

  server.on 'job.state', (job) ->
    msg = job.jsonableState()
    for res in connections when res.jobs is null or res.jobs.indexOf(job.id) != -1
      res.sse('job', msg)

  server.on 'submitted', (job) ->
    msg = job.jsonableState()
    for res in connections when res.jobs is null
      res.sse('job', msg)

  app.get '/jobs', (req, res) ->
    console.log('jobs')
    res.format
      'application/json': ->
        res.send(server.jsonableState())

      'text/html': ->
        res.sendfile(index_page)

      'text/event-stream': ->
        subscribe(req, res, null)

  app.get '/jobs/:id', (req, res) ->
    server.job req.params.id, (job) ->
      unless job
        return res.status(404).end("Not found")

      res.format
        'application/json': ->
          res.send(job.jsonableState())

        'text/html': ->
          res.sendfile(index_page)

  app.get '/jobs/:id/related', (req, res) ->
    server.relatedJobs req.params.id, (jobs) ->
      res.format
        'application/json': ->
          res.send(job.jsonableState() for job in jobs)

        'text/html': ->
          res.sendfile(index_page)

        'text/event-stream': ->
          subscribe(req, res, jobs)

  app.get '/jobs/:id/log', (req, res) ->
    server.job req.params.id, (job) ->
      unless job?.ctx
        return res.status(404).end("Not found")

      res.format
        'text/plain': ->
          res.send(job.ctx.log)

        'text/event-stream': ->
          serverEvent(req, res)
          s = new SSEStream()
          job.ctx.pipeAll(s)
          s.pipe(res)

          res.on 'close', ->
            job.ctx.unpipe(s)

  app

if module is require.main
  jobserver = require './index'
  server = new jobserver.Server()
  e1 = new jobserver.SeriesExecutor(new jobserver.Executor())
  e2 = new jobserver.SeriesExecutor(new jobserver.Executor())
  web(server).listen(8080)

  n = 0

  setInterval (->
    j = new jobserver.Job()
    j.executor = if n & 1 then e1 else e2
    j.name = "test"
    j.description = "Test Job #{n += 1}"
    j.run = (ctx) ->
      ctx.then (cb) ->
        ctx.write("\x1b[32mFoo\rStart\x1b[39m\n")
        setTimeout(cb, 7000 * Math.random() + 1000)
      ctx.then (cb) ->
        i = 0
        t = setInterval (->
          ctx.write "remote: Compressing objects: #{i}% (1/34)   \x1b[K\r"
          i+=1
          if i == 100
            clearInterval(t)
            ctx.write "remote: Compressing objects: 100% (1/1), done.\x1b[K\r\n"
            cb()
        ), 10
    server.submit(j)
  ), 4000
