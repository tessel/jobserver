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
	exports(server).listen(8080)

	setInterval (-> 
		j = new jobserver.Job()
		j.name = "test"
		j.description = "Test Job"
		j.doExec = (cb) ->
			setTimeout (-> cb(true)), 4000
		server.submit(j)
	), 5000

