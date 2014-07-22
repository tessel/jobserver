window.app = {}
_.extend(app, Backbone.Events)

class JobModel extends Backbone.Model
	logs: ->
		console.log ("/jobs/#{@id}/logs")
		new EventSource("/jobs/#{@id}/log")

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
			
			@$el.click =>
				app.selectJob(@model)

		if @model.get('settled')
			setTimeout (=> @$el.slideUp(500, => @remove())), 4000

		@$el

app.selectJob = (job) ->
	app.selectedJob = job
	app.trigger('selectedJob', job)

class JobSidebar extends Backbone.View
	initialize: ->
		@listenTo app, 'selectedJob', @render
		@logs = null
		
	render: ->
		@$('#title').empty().append(app.selectedJob.get 'description')
		@logs.close() if @logs
		@logs = app.selectedJob.logs()
		@logs.addEventListener 'open', =>
			console.log('open log')
			@$('#log').empty()
		@logs.addEventListener 'message', (e) =>
			console.log('data', e.data)
			@$('#log').append(document.createTextNode(JSON.parse(e.data)))
		@logs.addEventListener 'error', (e) ->
			console.log('error', e)
		@logs.addEventListener 'end', (e) =>
			console.log("stream end")
			@logs.close()

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
		
	sidebar = new JobSidebar({el: '#info'})

$().ready ->
	app.jobs = new JobCollection()
	app.list = new ListView()

	connect()
