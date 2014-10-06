jobserver = require './index'

@GitRef = class GitRef
  constructor: (@repo, @branch, @ref) ->
  serialize: -> JSON.stringify(this)
  @deserialize: (v) ->
    {repo, branch, ref} = JSON.parse(v)
    new GitRef(repo, branch, ref)

  refStr: ->
    ref = @ref.slice(0, 8)
    if @branch
      "#{@branch}@#{ref}"
    else
      ref

@GithubRepository = class GithubRepository
  constructor: (@github, @user, @repo) ->

  gitUrl: -> "https://github.com/#{@user}/#{@repo}.git"
  branchRef: (branch, cb) ->
    console.log 'branchref', branch
    if branch instanceof GitRef
      return cb(branch)

    @github.gitdata.getReference {@user, @repo, ref: "heads/#{branch}"}, (err, res) =>
      console.log(new GitRef(@gitUrl(), branch, res.object.sha))
      return cb(err) if err
      cb(new GitRef(@gitUrl(), branch, res.object.sha))

  forceReference: (branch, sha, cb) ->
    @github.gitdata.updateReference {@user, @repo, ref:"heads/#{branch}", sha, force:true}, (err, res) =>
      if err
        @github.gitdata.createReference {@user, @repo, ref:"refs/heads/#{branch}", sha}, (err) =>
          cb(err, sha)
      else
        cb(null, sha)

  forcePush: (branch, ref_branch, cb) ->
    @github.gitdata.getReference {@user, @repo, ref: "heads/#{ref_branch}"}, (err, res) =>
      return cb(err) if err
      @forceReference(branch, res.object.sha, cb)

  updateSubmodule: (base_branch, branch, path, commit, message, cb) ->
    @github.gitdata.getReference {@user, @repo, ref: "heads/#{base_branch}"}, (err, res) =>
      return cb(err) if err
      console.log(res)
      base_commit = res.object.sha

      @github.gitdata.getCommit {@user, @repo, sha:base_commit}, (err, res) =>
        return cb(err) if err
        console.log(res)
        base_tree = res.tree.sha

        @github.gitdata.createTree {@user, @repo, base_tree, tree:[{path, sha:commit, mode:'160000', type:'commit'}]}, (err, res) =>
          return cb(err) if err
          console.log(res)
          new_tree = res.sha

          @github.gitdata.createCommit {@user, @repo, parents:[base_commit], tree: new_tree, message}, (err, res) =>
            return cb(err) if err
            console.log(res)
            new_commit = res.sha
            @githubForceReference @user, @repo, branch, new_commit, (err) =>
              cb(err, new_commit)

  merge: (branch, reference, commit_message, cb) ->
    @github.repos.merge {@user, @repo, base: branch, head: reference, commit_message}, (err, res) =>
      cb(err, res?.sha)

  fastForward: (branch, sha, cb) ->
    @github.gitdata.updateReference {@user, @repo, ref:"heads/#{branch}", sha}, cb

  getPR: (number, cb) ->
    @github.pullRequests.get {@user, @repo, number}, cb

  inputFilter: ->
    (v, cb) => @getRef(v, cb)


@TestMergeRepo = class TestMergeRepo extends GithubRepository
  try_branch: 'incoming'
  master_branch: 'master'

  constructor: (github, user, repo, resource) ->
    super(github, user, repo)
    @resource = new jobserver.SeriesResource(resource)

  testJob: ->
    throw new Error("Unimplemented")

  mergeJob: (pr, expected, reviewer, cb) ->
    job = new jobserver.Job(@resource, {pr, expected, reviewer})
    job.name = "github-merge-#{@user}-#{@repo}"
    job.description = "Merge #{@user}/#{@repo} PR\##{pr} r=#{reviewer}"
    job.run = @runMerge.bind(this)

    job.withId =>
      body = "Approved by @#{reviewer}. [Running tests](#{job.url()})."
      @github.issues.createComment {@user, @repo, number:pr, body}
      console.trace('withid')

    job.on 'settled', =>
      console.trace('settled')
      if job.state != 'success'
        body = "[Merge or tests failed](#{job.url()})."
        @github.issues.createComment {@user, @repo, number:pr, body}

    cb(job)

  runMerge: (ctx) ->
    number = ctx.job.inputs.pr
    expected = ctx.job.inputs.expected
    reviewer = ctx.job.inputs.reviewer

    message = null
    merge_sha = null

    [
      (ctx, cb) =>
        @getPR number, (err, data) =>
          unless err
            if data.head.sha != expected
              ctx.write "Expected: #{expected}\n"
              ctx.write "Found: #{data.head.sha}\n"
              return cb("Pull request has been updated")
            else if data.merged
              return cb("Pull request has already been merged")
            else
              ctx.prData = data
              message = "Merge \##{number} r=@#{reviewer}: #{data.title}\n\n#{data.body}"
          cb(err)

      (ctx, cb) =>
        ctx.write "Resetting #{@try_branch} to #{@master_branch}\n"
        @forcePush @try_branch, @master_branch, cb

      (ctx, cb) =>
        ctx.write "Merging #{expected} into #{@try_branch}\n"
        @merge @try_branch, expected, message, (err, sha) =>
          unless err
            ctx.merge_sha = merge_sha = sha
            ctx.write "Merge succeeded, testing candidate #{merge_sha}\n"
          cb(err)

      if @runTest
        @runTest
      else
        (ctx, cb) =>
          @testJob @try_branch, (job) ->
            ctx.do ctx.runJob(job), cb

      (ctx, cb) =>
        ctx.write "Fast-forwarding #{@master_branch} to #{merge_sha}\n"
        @fastForward @master_branch, merge_sha, cb

      @afterMerge
    ]

  runTest: null
  afterMerge: null
