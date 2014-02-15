/**
 * This file implements a polling worker that should do just fine on AWS.
 * It's nothing fancy, so we'll see what happens.
 */

var program       = require('commander');
var Promise       = require('promise');
var request       = require('superagent');
var fs            = require('fs');
var nconf         = require('nconf');
var utils         = require('./cli/utils');
var spawn         = require('child_process').spawn;
var mime          = require('mime');


require('./utils/spread-promise').patch();

/** Create state.json with workerType, workerGroup and workerId */
var initialize = function () {
  // Get AMI Identifier as worker-type
  var amiId = new Promise(function(accept, reject) {
    request
      .get('http://169.254.169.254/latest/meta-data/ami-id')
      .end(function(res) {
        if (res.ok) {
          accept(res.text);
        } else {
          reject(res.status);
        }
      });
  });
  // Get availability zone as worker-group
  var availabilityZone = new Promise(function(accept, reject) {
    request
      .get('http://169.254.169.254/latest/meta-data/placement/availability-zone')
      .end(function(res) {
        if (res.ok) {
          accept(res.text);
        } else {
          reject(res.status);
        }
      });
  });
  // Get instance id as worker-id
  var instanceId = new Promise(function(accept, reject) {
    request
      .get('http://169.254.169.254/latest/meta-data/instance-id')
      .end(function(res) {
        if (res.ok) {
          accept(res.text);
        } else {
          reject(res.status);
        }
      });
  });

  // When above is done, create and save state
  return Promise.all(
    amiId,
    availabilityZone,
    instanceId
  ).spread(function(amiId, availabilityZone, instanceId) {
    // Create state as follows
    var state = {
      workerType:     amiId,
      workerGroup:    availabilityZone,
      workerId:       instanceId
    };

    // Save state to disk
    utils.saveState(state);
  });
};


/** Claim a task from queue.taskcluster.net */
var claimWork = function() {
  // First load state
  var state = utils.loadState();

  // Get next
  return new Promise(function(accept, reject) {
    request
      .get(utils.queueUrl('/claim-work/' + state.provisionerId + '/' + state.workerType))
      .end(function(res) {
        if (res.status == 200) {
          state.taskId      = res.body.status.taskId;
          state.runId       = res.runId;
          state.takenUntil  = res.body.status.takenUntil;
          state.logsUrl     = res.body.logsPutUrl;
          state.resultUrl   = res.body.resultPutUrl;
          utils.saveState(state);
          accept();
        } else {
          reject();
        }
      });
  });
};

/** Fetch task.json into state.task */
var fetchTask = function() {
  // First load state
  var state = utils.loadState();

  return new Promise(function(accept, reject) {
    request
      .get('http://tasks.taskcluster.net/' + state.taskId + '/task.json')
      .end(function(res) {
        if(res.ok) {
          state.task = res.body;
          utils.saveState(state);
        } else {
          reject();
        }
      });
  });
};


/** Reclaim task and return Date object for new takenUntil */
var reclaim = function() {
  var state = utils.loadState();
  return new Promise(function(accept, reject) {
    request
      .post(utils.queueUrl('/task/' + state.taskId + '/claim'))
      .send({
        workerGroup:      state.workerGroup,
        workerId:         state.workerId,
        runId:            state.runId
      })
      .end(function(res) {
        if (res.ok) {
          state.takenUntil  = res.body.status.takenUntil;
          state.logsUrl     = res.body.logsPutUrl;
          state.resultUrl   = res.body.resultPutUrl;
          utils.saveState(state);
          accept(new Date(res.body.status.takenUntil));
        } else {
          reject();
        }
      });
  });
};

/** Run task from state.json */
var runTask = function() {
  // First load state
  var state = utils.loadState();

  // Create some log files
  var stdout = fs.openSync('stdout.log', 'w');
  var stderr = fs.openSync('stderr.log', 'w');

  // Spawn task
  var task = spawn(
    state.task.command,
    state.task.arguments,
    {
      stdio: ['ignore', stdout, stderr]
    }
  );

  var takenUntil = new Date(state.takenUntil);
  var reclaim_timeout = null;
  var setReclaimTimeout = function() {
    reclaim_timeout = setTimeout(function() {
      reclaim().then(function(newTakenUntil) {
        var takenUntil = newTakenUntil;
        setReclaimTimeout();
      }, function() {
        // TODO: This is a little aggressive, we should allow it to fail a few
        // times before we kill... And we should check the error code, 404
        // Task not found, means task completed or canceled, in which case we
        // really should kill()
        task.kill();
      });
    },
      takenUntil.getTime() - (new Date()).getTime() - 1000 * 60 * 3
    );
  };

  // Record the starting time
  var started = new Date();

  // Promise that task completes
  return new Promise(function(accept, reject) {
    task.on('close' function(code) {
      var result = null;
      if (code !== 0) {
        result = {
          message:      "Task completed unsuccessfully",
          exitcode:     code
        };
      } else {
        result = {
          message:      "Task completed successfully",
          exitcode:     code
        };
      }
      // Write result.json file that can be uploaded to S3
      fs.writeFileSync('result.json', JSON.stringify({
        "version":            "0.2.0",
        "artifacts": {
          // No artifacts, we have no logic of recording this yet... Ideally
          // all artifacts that is uploaded through put-artifact from the worker
          // should be listed here...
        },
        "statistics": {
          "started":          started.toJSON(),
          "finished":         (new Date()).toJSON()
        },
        "worker": {
          "workerGroup":      state.workerGroup,
          "workerId":         state.workerId
        }
        // This is tasks specific results
        "result":             result
      }, undefined, 4), {encoding: 'utf-8'});
      // Clear timeout for reclaims
      clearTimeout(reclaim_timeout);
      fs.closeSync(stdout);
      fs.closeSync(stderr);
      accept();
    });
  });
};

// Auxiliary function that puts a file to a URL
var putToUrl = function(file, url, contentType) {
  return new Promise(function(accept, reject) {
    var stat = fs.statSync(file);
    var req = request
                .put(url)
                .set('content-Type',    contentType)
                .set('content-Length',  stat.size);
    fs.createReadStream(file).pipe(req, {end: false});
    req.end(function(res) {
      if(res.ok) {
        accept();
      } else {
        reject();
      }
    });
  });
};

/** Upload logs, result and report task completed */
var completeTask = function() {
  var state = utils.loadState();

  // Get artifact URLs for stderr and stdout
  var log_urls = new Promise(function(accept, reject) {
    request
      .post(utils.queueUrl('/task/' + state.taskId + '/artifact-urls'))
      .send({
        workerGroup:      state.workerGroup,
        workerId:         state.workerId,
        runId:            state.runId,
        artifacts: {
          "stderr.log":   {contentType: "text/plain"},
          "stdout.log":   {contentType: "text/plain"}
        }
      })
      .end(function(res) {
        if(res.ok) {
          accept(res.body.artifactPutUrls);
        } else {
          reject();
        }
      }
  });

  // Write the logs.json file
  fs.writeFileSync('logs.json', JSON.stringify({
    version:  "0.2.0",
    logs: {
      'stdout.log': "http://tasks.taskcluster.net/" + state.taskId + "/runs/" +
                    state.runId + "/artifacts/stdout.log",
      'stderr.log': "http://tasks.taskcluster.net/" + state.taskId + "/runs/" +
                    state.runId + "/artifacts/stderr.log"
    }
  }, undefined, 4), {encoding: 'utf-8'});

  // Upload files
  var files_uploaded = log_urls.then(function(putUrls) {
    return Promise.all(
      putToUrl('stdout.log',  putUrls['stdout.log'],  'text/plain'),
      putToUrl('stderr.log',  putUrls['stderr.log'],  'text/plain'),
      putToUrl('logs.json',   state.logsUrl,          'application/json'),
      putToUrl('result.json', state.resultUrl,        'application/json')
    );
  });

  // When files are uploaded report the task as completed
  return files_uploaded.then(function() {
    return new Promise(function(accept, reject) {
      request
        .post(utils.queueUrl('/task/' + state.taskId + '/completed'))
        .send({
          workerGroup:      state.workerGroup,
          workerId:         state.workerId,
          runId:            state.runId
        })
        .end(function(res) {
          if(res.ok) {
            accept();
          } else {
            reject();
          }
        });
    });
  });
};

// Find worker-type from meta-data service
// Claim-work from queue.taskcluster.net
// Fetch task.json from tasks.taskcluster.net
// mkdir work-folder
// run command
// rm -rf work-folder
// Put logs.json and result.json
// Report task complete

var main = function() {};
var claim_tries = 10;
var workLoop = function() {
  claimWork().then(function() {
    claim_tries = 10;

    // Fetch, run and compete task
    fetchTask.then(function() {
      return runTask();
    }).then(function() {
      return completeTask();
    }).then(function() {
      // Start the work loop again
      workLoop();
    }, function(err) {
      console.log("Error while running loop");
      console.log(err);
    });
  }, function() {
    // Exit and shutdown... if we're out of claim_tries
    if (claim_tries > 0) {
      claim_tries -= 1;
      setTimeout(workLoop, 60 * 1000);
    } else {
      spawn('sudo', ['shutdown', '-h', 'now']);
    }
  });
}

program
  .command('start')
  .action(function() {
    initialize().then(function() {
      workLoop();
    });
  });

program
  .command('put-artifact <name> <file>')
  .description("Put an artifact for run-id and task-id in state.json")
  .action(function(name, file, options) {

  // Load state, we'll need this for the request
  var state = utils.loadState();

  // Test that specified file exists
  var stat = fs.statSync(file);
  if (!stat.isFile()) {
    console.log(("No such file: " + file.bold).red);
    process.exit(1);
  }

  // Find mimetype of file to upload
  contentType = mime.lookup(file);

  // Create artifacts map to submit
  var artifacts = {};
  artifacts[name] = {
    contentType:       contentType
  };

  // Request artifact put urls
  request
    .post(utils.queueUrl('/task/' + state.taskId + '/artifact-urls'))
    .send({
      workerGroup:      state.workerGroup,
      workerId:         state.workerId,
      runId:            state.runId,
      artifacts:        artifacts
    })
    .end(function(res) {
      if (res.ok) {
        console.log("Fetched signed S3 put URL from queue");
        var req = request
                    .put(res.body.artifactPutUrls[name])
                    .set('Content-Type',    contentType)
                    .set('Content-Length',  stat.size)
        fs.createReadStream(file).pipe(req, {end: false});
        req.end(function(res) {
          console.log(res.status);
          if (res.ok) {
            console.log("Upload successful".bold);
          } else {
            console.log("Upload to signed url failed".bold.red);
          }
        });
      } else {
        console.log("Failed get a signed url, errors:".bold.red);
        console.log(cliff.inspect(res.body));
      }
    });
});