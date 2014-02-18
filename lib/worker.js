var Promise       = require('promise');
var request       = require('superagent');
var fs            = require('fs');
var mime          = require('mime');
var debug         = require('debug')('worker');
var _             = require('lodash');
var nconf         = require('nconf');


/** Get a URL for an API end-point on the queue */
var queueUrl = function(path) {
  return 'http://' + nconf.get('queue:hostname') + ':' +
          nconf.get('queue:port') + '/v1' + path;
};

/**
 * Create a worker with options as:
 * `{provisionerId, workerType, workerGroup, workerId}`
 */
var Worker = function(options) {
  if (!options.provisionerId ||
      !options.workerType ||
      !options.workerGroup ||
      !options.workerId) {
    debug("Failed to create Worker Instance, keys in options are missing!");
    throw new Error(
      "Options must specify provisionerId, workerType, workerGroup and " +
      "workerId"
    );
  }
  this.options = options;
  this.clearState();
  debug("Created Worker instance");
};

/**
 * Claim a task from queue and fetch it, returns a promise for a boolean that
 * is `true` if a task was claimed, and `false` if no task is available.
 *
 * Warning, this will clear internal state, regardless of success and
 * `clearKeepTask()` will be called, so if you wish to keep the task claimed
 * after this method, you will need to call `keepTask()`, when this is done.
 */
Worker.prototype.claimWork = function() {
  // Warn if we're clearing previous state
  if (this.hasTask()) {
    debug('Worker.claimWork, Warning: Discarding previous state!');
  }
  this.clearState();

  var that = this;
  return new Promise(function(accept, reject) {
    // Create claim-work URL
    var url = queueUrl(
      '/claim-work/' + that.options.provisionerId + '/' +
      that.options.workerType
    );

    debug("GET: %s", url);
    // First /claim-work/...
    request
      .get(url)
      .send({
        workerGroup:    that.options.workerGroup,
        workerId:       that.options.workerId
      })
      .end(function(res) {
        if (res.status == 200) {
          accept({claimedTask: true, reply: res.body});
        } else if (res.status == 204) {
          accept({claimedTask: false, reply: res.body});
        } else {
          debug(
            "Failed to /claim-work/..., error: %s, as JSON: %j",
            res.text, res.body
          );
          reject();
        }
      });
  }).then(function(result) {
    if (!result.claimedTask) {
      return false;
    }
    return new Promise(function(accept, reject) {
      // Then we fetch the tasks from S3
      var url = 'http://tasks.taskcluster.net/' + result.reply.status.taskId +
                '/task.json';
      request
        .get(url)
        .end(function(res) {
          if(res.ok) {
            debug("Task claimed: %s", result.reply.status.taskId);
            accept(res.body);
          } else {
            debug("Failed to fetch task.json from tasks.taskcluster.net");
            reject();
          }
        });
    }).then(function(task) {
      that._status        = result.reply.status;
      that._task          = task;
      that._runId         = result.reply.runId;
      that._logsPutUrl    = result.reply.logsPutUrl;
      that._resultPutUrl  = result.reply.resultPutUrl;
      return true;
    });
  });
};

/** True, if the worker has a task, and `takenUntil` have expired */
Worker.prototype.hasTask = function() {
  return this._status !== null;
};

/**
 * Reclaim task, current task, returns a promise of success
 *
 * **Note**, consider using `keepTask()` and `clearKeepTask()` instead of
 * reimplementing the timing logic.
 */
Worker.prototype.reclaimTask = function() {
  var that = this;
  return new Promise(function(accept, reject) {
    var url = queueUrl('/task/' + that._status.taskId + '/claim');
    debug("POST: %s", url);
    request
      .post(url)
      .send({
        workerGroup:      that.options.workerGroup,
        workerId:         that.options.workerId,
        runId:            that._runId
      })
      .end(function(res) {
        if (res.ok) {
          debug("Successfully, reclaimed task");
          that._status        = res.body.status;
          that._logsPutUrl    = res.body.logsPutUrl;
          that._resultPutUrl  = res.body.resultPutUrl;
          accept();
        } else {
          debug("Failed to reclaim task!");
          reject();
        }
      });
  });
};

/**
 * Keep task by updating reclaiming task from queue before `takenUntil` expires,
 * until `taskCompleted()` is called or `clearKeepTask()` is called.
 *
 * The optional argument `abortCallback` will be called if a reclaim fails.
 */
Worker.prototype.keepTask = function(abortCallback) {
  var that = this;
  var setReclaimTimeout = function() {
    that._reclaimTimeoutHandle = setTimeout(function() {
      that.reclaimTask().then(setReclaimTimeout, function() {
        // TODO: This is a little aggressive, we should allow it to fail a few
        // times before we abort... And we should check the error code, 404
        // Task not found, means task completed or canceled, in which case we
        // really should abort immediately
        if (abortCallback) {
          abortCallback();
        }
      });
    },
      (new Date(that._status.takenUntil)).getTime() -
      (new Date()).getTime() - 1000 * 60 * 3
    );
  };
};

/** Stop reclaiming from the queue before `takenUntil` expires */
Worker.prototype.clearKeepTask = function() {
  if(this._reclaimTimeoutHandle) {
    clearTimeout(this._reclaimTimeoutHandle);
    this._reclaimTimeoutHandle = null;
  }
};

/**
 * Returns task status from cache
 */
Worker.prototype.status = function() {
  return _.cloneDeep(this._status);
};

/** Get task definition from cache */
Worker.prototype.task = function() {
  return _.cloneDeep(this._task);
};

/** Put logs.json for current task, returns promise of success */
Worker.prototype.putLogs = function(json) {
  var that = this;
  return new Promise(function(accept, reject) {
    debug("Uploading logs.json to signed PUT URL");
    request
      .put(that._logsPutUrl)
      .send(json)
      .end(function(res) {
        if(res.ok) {
          debug("Successfully, uploaded logs.json");
          accept();
        } else {
          debug("Failed to upload logs.json, error: %s", res.text)
          reject();
        }
      });
  });
};

/** Put result.json for current task, returns promise of success */
Worker.prototype.putResult = function(json) {
  var that = this;
  return new Promise(function(accept, reject) {
    debug("Uploading result.json to PUT URL");
    request
      .put(that._resultPutUrl)
      .send(json)
      .end(function(res) {
        if(res.ok) {
          debug("Successfully, uploaded result.json");
          accept();
        } else {
          debug("Failed to upload logs.json, error: %s", res.text)
          reject();
        }
      });
  });
};

/**
 * Put artifact from file, returns promise for a URL to the uploaded artifact
 *
 * If the optional contentType isn't provided, Content-Type will be deduced from
 * filename.
 */
Worker.prototype.putArtifact = function(name, filename, contentType) {
  var that = this;
  return new Promise(function(accept, reject) {
    // Test that specified file exists
    var stat = fs.statSync(filename);
    if (!stat.isFile()) {
      throw new Error("No such file: " + filename);
    }

    // Lookup mimetype if not provided
    if (!contentType) {
      contentType = mime.lookup(filename);
    }

    // Create artifacts map to submit
    var artifacts = {};
    artifacts[name] = {
      contentType:       contentType
    };

    // Construct request URL for fetching signed artifact PUT URLs
    var url = queueUrl('/task/' + that._status.taskId + '/artifact-urls');

    // Request artifact put urls
    debug("POST: %s", url);
    request
      .post(url)
      .send({
        workerGroup:      that.options.workerGroup,
        workerId:         that.options.workerId,
        runId:            that._runId,
        artifacts:        artifacts
      })
      .end(function(res) {
        if (res.ok) {
          debug("Got signed artifact PUT URL from queue");
          var req = request
                      .put(res.body.artifactPutUrls[name])
                      .set('Content-Type',    contentType)
                      .set('Content-Length',  stat.size);
          fs.createReadStream(filename).pipe(req, {end: false});
          req.end(function(res) {
            if (res.ok) {
              debug("Successfully uploaded artifact %s to PUT URl", name);
              var artifactUrl = 'http://tasks.taskcluster.net/' +
                                that._status.taskId + '/runs/' + that._runId +
                                '/artifacts/' + name;
              accept(artifactUrl);
            } else {
              debug("Failed to upload to signed artifact PUT URL");
              reject();
            }
          });
        } else {
          debug("Failed get a signed artifact URL, errors: %s", res.text);
          reject();
        }
      });
  });
};

/** Report task completed, returns promise of success */
Worker.prototype.taskCompleted = function() {
  var that = this;
  return new Promise(function(accept, reject) {
    var url = queueUrl('/task/' + that._status.taskId + '/completed');
    debug("POST: %s", url);
    request
      .post(url)
      .send({
        workerGroup:      that.options.workerGroup,
        workerId:         that.options.workerId,
        runId:            that._runId
      })
      .end(function(res) {
        if(res.ok) {
          debug("Successfully reported task completed");
          accept();
        } else {
          debug("Failed to report task as completed, error code: %s", res.status);
          reject();
        }
      });
  }).then(function() {
    // Clear state
    that.clearState();
  });
};

/**
 * Save worker state to JSON
 * The fact that keepTask is set, will not be stored in the state.
 * This is mainly useful for restoring from crashes and transferring state
 * between processes, if another process were to put an artifact.
 */
Worker.prototype.saveState = function() {
  return _.cloneDeep({
    status:           this._status,
    task:             this._task,
    runId:            this._runId,
    logsPutUrl:       this._logsPutUrl,
    resultPutUrl:     this._resultPutUrl
  });
};

/** Load state created by saveState, if keepTask is set this will be cleared */
Worker.prototype.loadState = function(json) {
  json = _.cloneDeep(json);
  this._status        = json.status;
  this._task          = json.task;
  this._runId         = json.runId;
  this._logsPutUrl    = json.logsPutUrl;
  this._resultPutUrl  = json.resultPutUrl;
  this.clearKeepTask();
};

/** Clear internal state of worker */
Worker.prototype.clearState = function() {
  // Clear state
  this._status        = null;
  this._task          = null;
  this._runId         = null;
  this._logsPutUrl    = null;
  this._resultPutUrl  = null;
  this.clearKeepTask();
};

module.exports = Worker;