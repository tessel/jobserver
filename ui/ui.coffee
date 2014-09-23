window.app = {}
_.extend(app, Backbone.Events)

logparse = require('./logparse')

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
    @listenTo app, 'selectedJob', this.updateSelection

  render: ->
    @$el.attr('data-status', @model.get 'state')
    unless @$header
      @$el.empty().addClass('job')
      @$header = $("<header>").appendTo(@$el)
      @$title = $("<h1>").text(@model.get('description') or @model.get('name')).appendTo(@$header)

      @$el.click =>
        app.selectJob(@model)

      @$el.dblclick =>
        app.router.navigate("/jobs/#{@model.id}", true)

    @$el

  updateSelection: (j) ->
    @$el.toggleClass('selected', j.id is @model.id)

app.selectJob = (job) ->
  app.selectedJob = job
  app.trigger('selectedJob', job)

class JobSidebar extends Backbone.View
  initialize: ->
    @listenTo app, 'selectedJob', @selection
    @logs = null

  selection: ->
    @stopListening @model, 'change'
    @model = app.selectedJob
    if @model
      @render()
      @listenTo @model, 'change', @update

  render: ->
    @$('#title').empty().append(@model.get 'description')

    @update()

    @logs.close() if @logs
    @$('#log').empty()
    @logs = app.selectedJob.logs()
    @logs.addEventListener 'open', =>
      console.log('open log')
      @logparse = new logparse(@$('#log')[0])
    @logs.addEventListener 'message', (e) =>
      @logparse.push(JSON.parse(e.data))
    @logs.addEventListener 'error', (e) ->
      console.log('error', e)
    @logs.addEventListener 'end', (e) =>
      console.log("stream end")
      @logs.close()

  showList = (elem, dict) ->
    console.log(dict)
    elem.empty().hide()
    for k, v of dict when v
      elem.show()
      $('<dt>').text(k).appendTo(elem)
      if v.blob
        a = $("<a>").text('[download]')
          .attr('href', "/blob/#{v.id}")
          .attr('download', k)
        $("<dd>").append(a).appendTo(elem)
      else
        $("<dd>").text(JSON.stringify(v)).appendTo(elem)

  update: ->
    showList(@$('#inputs'), @model.get('inputs'))
    showList(@$('#results'), @model.get('results'))

connect = (path, id) ->
  if app.eventsource
    app.eventsource.close()

  app.eventsource = new EventSource(path)

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
    app.selectJob((id and app.jobs.get(id)) or app.jobs.at(0))

  listen 'job', (m) ->
    app.jobs.add(m, {merge: true})



class Router extends Backbone.Router
  routes:
    '': 'allJobs'
    'jobs': 'allJobs'
    'jobs/:id': 'relatedJobs'

  allJobs: ->
    connect('/jobs')

  relatedJobs: (id) ->
    connect("/jobs/#{id}/related", id)


$().ready ->
  app.jobs = new JobCollection()
  app.list = new ListView()
  app.sidebar = new JobSidebar({el: '#info'})
  app.router = new Router()
  Backbone.history.start({pushState: true});
