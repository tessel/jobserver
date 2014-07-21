express = require('express')
serverEvent = require('server-event')()

exports = (jobserver) ->
  app = express()
  app.use(express.static('ui'));

  app.get '/jobs', (req, res) ->
    res.format
      'application/json': ->
        res.send(jobserver.jsonableState())

      'text/html': ->
        res.sendfile('./ui/index.html')

      'text/event-stream': ->
        serverEvent(req, res)
        res.sse('hello', jobserver.jsonableState())

        sendJobUpdate = (job) ->
          res.sse('job', job.jsonableState())

        jobserver.on 'submitted', sendJobUpdate
        jobserver.on 'job.state', sendJobUpdate

  app.get '/jobs/:id', (req, res, id) ->
    res.format
      'application/json': ->
        res.send({ job: 'foo', id: req.params.id })

      'text/html': ->
        res.sendfile('./ui/index.html')

      'text/event-stream': ->
        serverEvent(req, res)

  app.get '/jobs/:id/log', (req, res, id) ->
    res.format
      'text/plain': ->
        res.send('log')

      'text/event-stream': ->
        serverEvent(req, res)

  app

if module is require.main
  jobserver = require './jobserver'

  jobstore = new jobserver.JobStoreMem()
  blobstore = new jobserver.BlobStoreMem()
  server = new jobserver.Server(jobstore, blobstore)
  e1 = new jobserver.SeriesExecutor(new jobserver.Executor())
  e2 = new jobserver.SeriesExecutor(new jobserver.Executor())
  exports(server).listen(8080)

  n = 0

  setInterval (->
    j = new jobserver.Job()
    j.executor = if n & 1 then e1 else e2
    j.name = "test"
    j.description = "Test Job #{n += 1}"
    j.run = (ctx) ->
      setTimeout (-> ctx.done()), 3000 * Math.random() + 1000
    server.submit(j)
  ), 1000
