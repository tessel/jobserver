http = require('http')
WebSocketServer = require('ws').Server
sendStatic = require('send')

exports = (jobserver) ->
	httpserver = http.createServer (req, res) ->
		sendStatic(req, req.url).from(__dirname + '/ui').pipe(res)

	wss = new WebSocketServer {server: httpserver, path: '/socket'}

	sendToAll = (packet) ->
		for ws in wss.clients
			ws.send(JSON.stringify(packet), -> )

	wss.on 'connection', (ws) ->
		ws.send(JSON.stringify({event: 'hello', state: jobserver.jsonableState()}))

	sendJobUpdate = (job) ->
		sendToAll({event: 'job', job: job.jsonableState()})

	jobserver.on 'submitted', sendJobUpdate
	jobserver.on 'job.state', sendJobUpdate

	httpserver

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
		j.doExec = (cb) ->
			setTimeout (-> cb(true)), 3000 * Math.random() + 1000
		server.submit(j)
	), 1000
