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
  app.use(express.static('ui'));

  app.get '/jobs', (req, res) ->
    console.log('jobs')
    res.format
      'application/json': ->
        res.send(server.jsonableState())

      'text/html': ->
        res.sendfile('./ui/index.html')

      'text/event-stream': ->
        serverEvent(req, res)
        res.sse('hello', server.jsonableState())

        sendJobUpdate = (job) ->
          res.sse('job', job.jsonableState())

        server.on 'submitted', sendJobUpdate
        server.on 'job.state', sendJobUpdate

  app.get '/jobs/:id', (req, res) ->
    job = server.job(req.params.id)
    
    res.format
      'application/json': ->
        res.send({ job: 'foo', id: req.params.id })

      'text/html': ->
        res.sendfile('./ui/index.html')

      'text/event-stream': ->
        serverEvent(req, res)

  app.get '/jobs/:id/log', (req, res) ->
    job = server.job(req.params.id)
    
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

  app

if module is require.main
  jobserver = require './jobserver'

  jobstore = new jobserver.JobStoreMem()
  blobstore = new jobserver.BlobStoreMem()
  server = new jobserver.Server(jobstore, blobstore)
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
      ctx.write("Start\n")
      setTimeout (->
        ctx.write("Done\n")
        ctx.done()
      ), 8000 * Math.random() + 1000
    server.submit(j)
  ), 4000
