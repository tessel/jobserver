module.exports = class LogParse
  constructor: (@elem) ->
    @elem.innerHTML = ''
    @beginLine()

    @state = 'text'
    @chunk = ''
    @control = ''

    @foldDepth = 0

    @ansi = {
      foreground: null
      background: null
      bold: false
      italic: false
      underline: false
    }

  beginLine: ->
    @pre = document.createElement('pre')
    @line = document.createElement('div')
    @line.appendChild(@pre)
    @appended = false

  push: (s) ->
    for c in s
      switch @state
        when 'text'
          switch c
            when '\x1b'
              @state = 'ansiStart'
            when '\n'
              @replaceLine = false
              @endSpan()
              @endLine()
            when '\r'
              @endSpan()
              @replaceLine = true
            when '\x02'
              @foldDepth += 1
            when '\x03'
              @foldDepth -= 1
            else
              @chunk += c
        when 'ansiStart'
          switch c
            when '['
              @endSpan()
              @control = ''
              @state = 'ansi'
            else
              # invalid escape
              @state = 'text'
        when 'ansi'
          switch c
            when 'm'
              for code in @control.split(';')
                @ansiStyle(code)
              @control = ''
              @state = 'text'
            when '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', ';'
              @control += c
            else
              # ignore unknown command
              @state = 'text'

    @endSpan()
    @flushLine()

  ansiStyle: (code) ->
    if foregroundColors[code]
      @ansi.foreground = foregroundColors[code]
    else if backgroundColors[code]
      @ansi.background = backgroundColors[code]
    else if code == '39' then @ansi.foreground = null
    else if code == '49' then @ansi.background = null
    else if code == '1'  then @ansi.bold = true
    else if code == '22' then @ansi.bold = false
    else if code == '3'  then @ansi.italic = true
    else if code == '23' then @ansi.italic = false
    else if code == '4'  then @ansi.underline = true
    else if code == '24' then @ansi.underline = false

  endSpan: ->
    unless @chunk.length
      return

    if @replaceLine
      @pre.innerHTML = ''
      @replaceLine = false

    classes = []
    classes.push(@ansi.foreground) if @ansi.foreground
    classes.push('bg-'+ @ansi.background) if @ansi.background
    classes.push('bold') if @ansi.bold
    classes.push('italic') if @ansi.italic

    node = text = document.createTextNode(@chunk)
    if classes.length
      node = document.createElement('span')
      node.className = classes.join(' ')
      node.appendChild(text)
    @pre.appendChild(node)

    @chunk = ''

  endLine: ->
    @flushLine()
    @beginLine()

  flushLine: ->
    unless @appended
      @elem.appendChild @line
      @appended = true


foregroundColors =
  '30': 'black',
  '31': 'red',
  '32': 'green',
  '33': 'yellow',
  '34': 'blue',
  '35': 'magenta',
  '36': 'cyan',
  '37': 'white',
  '90': 'grey'


backgroundColors =
  '40': 'black',
  '41': 'red',
  '42': 'green',
  '43': 'yellow',
  '44': 'blue',
  '45': 'magenta',
  '46': 'cyan',
  '47': 'white'
