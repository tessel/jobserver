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
		app.jobs.each (job) => @addOne(job)

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
	app.eventsource = new EventSource('/jobs')

	app.eventsource.addEventListener 'open', ->
		console.log('open')

	app.eventsource.addEventListener 'close', ->
		console.log('close')

	app.eventsource.addEventListener 'error', (e) ->
		console.log('error', e)

	listen = (type, cb) ->
		app.eventsource.addEventListener type, (e) ->
			cb(JSON.parse(e.data))

	listen 'hello', (m) ->
		app.jobs.reset(m.jobs)

	listen 'job', (m) ->
		app.jobs.add(m, {merge: true})

$().ready ->
	app.jobs = new JobCollection()
	app.list = new ListView()

	connect()
