window.app = {}

class JobModel extends Backbone.Model

class JobCollection extends Backbone.Collection
	model: JobModel

class ListView extends Backbone.View
	el: '#list'

	initialize: ->
		@listenTo app.jobs, 'add', this.addOne
		@listenTo app.jobs, 'reset', this.addAll

	addAll: ->
		@$el.empty()

	addOne: (job) ->
		view = new JobTile { model: job }
		@$el.append view.render()

class JobTile extends Backbone.View
	initialize: ->
		@listenTo @model, 'change', this.render
		@listenTo @model, 'destroy', this.remove

	render: ->
		@$el.attr('data-status', @model.get 'state')
		unless @$header
			@$el.empty().addClass('job')
			@$header = $("<header>").appendTo(@$el)
			@$title = $("<h1>").text(@model.get 'description').appendTo(@$header)

		if @model.get('settled')
			setTimeout (=> @$el.slideUp(500, => @remove())), 2000

		@$el

class JobSidebar extends Backbone.View

connect = ->
	protocol = if window.location.protocol is 'https:' then 'wss:' else 'ws:'
	socket = new WebSocket("#{protocol}//#{window.location.host}#{window.location.pathname}socket")
	app.socket = socket

	socket.onopen = ->
		console.log('open')

	socket.onclose = ->
		console.log('close')

	socket.onerror = ->
		console.log('error')

	socket.onmessage = (msg) ->
		m = JSON.parse(msg.data)

		console.log(msg.data)

		switch m.event
			when 'hello'
				app.jobs.reset(m.jobs)
			when 'job'
				app.jobs.add(m.job, {merge: true})

$().ready ->
	app.jobs = new JobCollection()
	app.list = new ListView()

	connect()

