module.exports = (grunt) ->
  grunt.initConfig
    browserify:
      options:
        bundleOptions:
          debug: true
        browserifyOptions:
          extensions: ['.coffee']
        transform: ['coffeeify']
      ui:
        src: ['ui/**/*.coffee', 'ui/**/*.js', '!ui/app.js']
        dest:'ui/app.js'
    watch:
      app:
        files: 'ui/**/*.coffee'
        tasks: ['browserify']

  # These plugins provide necessary tasks.
  grunt.loadNpmTasks 'grunt-browserify'
  grunt.loadNpmTasks 'grunt-contrib-watch'

  # Default task.
  grunt.registerTask 'default', ['browserify']
