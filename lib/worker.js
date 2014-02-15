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
    throw new Error(
      "Options must specify provisionerId, workerType, workerGroup and " +
      "workerId"
    );
  }
  this.options = options;
  this.clearState();
};

/**
 * Claim a task from queue and fetch it, returns a promise of success
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
    // First /claim-work/...
    request
      .get(
        queueUrl('/claim-work/' + that.options.provisionerId + '/' +
        that.options.workerType)
      )
      .end(function(res) {
        if (res.status == 200) {
          accept(res.body);
        } else {
          reject();
        }
      });
  }).then(function(reply) {
    return new Promise(function(accept, reject) {
      // Then we fetch the tasks from S3
      request
        .get('http://tasks.taskcluster.net/' + reply.status.taskId + '/task.json')
        .end(function(res) {
          if(res.ok) {
            accept(res.body);
          } else {
            debug("Failed to fetch task.json from tasks.taskcluster.net");
            reject();
          }
        });
    }).then(function(task) {
      that._status        = reply.status;
      that._task          = task;
      that._runId         = reply.runId;
      that._logsPutUrl    = reply.logsPutUrl;
      that._resultPutUrl  = reply.resultPutUrl;
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
    request
      .post(queueUrl('/task/' + that._status.taskId + '/claim'))
      .send({
        workerGroup:      that.options.workerGroup,
        workerId:         that.options.workerId,
        runId:            that._runId
      })
      .end(function(res) {
        if (res.ok) {
          that._status        = res.body.status;
          that._logsPutUrl    = res.body.logsPutUrl;
          that._resultPutUrl  = res.body.resultPutUrl;
          accept();
        } else {
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
      (new Date()).getTime() - 1000 * 60 * 3;
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
    var data = new Buffer(JSON.stringify(json), 'utf8');
    request
      .put(that._logsPutUrl)
      .set('content-Type',    'application/json')
      .set('content-Length',  data.length);
      .send(data)
      .end(function(res) {
        if(res.ok) {
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
    var data = new Buffer(JSON.stringify(json), 'utf8');
    request
      .put(that._resultPutUrl)
      .set('content-Type',    'application/json')
      .set('content-Length',  data.length);
      .send(data)
      .end(function(res) {
        if(res.ok) {
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

    // Request artifact put urls
    request
      .post(queueUrl('/task/' + that._status.taskId + '/artifact-urls'))
      .send({
        workerGroup:      that.options.workerGroup,
        workerId:         that.options.workerId,
        runId:            that._runId,
        artifacts:        artifacts
      })
      .end(function(res) {
        if (res.ok) {
          var req = request
                      .put(res.body.artifactPutUrls[name])
                      .set('Content-Type',    contentType)
                      .set('Content-Length',  stat.size);
          fs.createReadStream(filename).pipe(req, {end: false});
          req.end(function(res) {
            if (res.ok) {
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
          debug("Failed get a signed artifact URL, errors:");
          reject();
        }
      });
  });
};

/** Report task completed, returns promise of success */
Worker.prototype.taskCompleted = function() {
  var that = this;
  return new Promise(function(accept, reject) {
    request
      .post(queueUrl('/task/' + that._status.taskId + '/completed'))
      .send({
        workerGroup:      that.options.workerGroup,
        workerId:         that.options.workerId,
        runId:            that._runId
      })
      .end(function(res) {
        if(res.ok) {
          accept();
        } else {
          reject();
        }
      });
  }).then(function() {
    // Clear state
    this.clearState();
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