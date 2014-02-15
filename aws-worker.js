var program       = require('commander');
var Promise       = require('promise');
var Worker        = require('./lib/worker');
var metadata      = require('./lib/aws-metadata');
var spawn         = require('child_process').spawn;
var debug         = require('debug')('aws-worker');

require('./utils/spread-promise').patch();

/** Process a task with the worker */
var processTask = function(worker) {
  // Claim work
  var started = null;
  var finished = null;
  return worker.claimWork().then(new Promise(function(accept, reject) {
    // Get task definition
    var task = worker.task();

    // Create some log files
    var stdout = fs.openSync('stdout.log', 'w');
    var stderr = fs.openSync('stderr.log', 'w');

    // Set time at which task started
    started = new Date();

    // Start worker process
    var process = spawn(
      task.payload.command,
      task.payload.arguments
      {stdio: ['ignore', stdout, stderr]}
    );

    // Keep tasks, and tell it to kill the process if reclaiming the task fails
    var aborted = false;
    worker.keepTask(function() {
      aborted = true;
      process.kill();
    });

    // When process is completed we are done
    process.on('close', function(exitcode) {
      // Set time as which task finished
      finished = new Date();

      // First close stdout and stderr handles
      fs.closeSync(stdout);
      fs.closeSync(stderr);

      if (aborted) {
        // If this process was aborted due to reclaim failures, we don't report
        // task as completed
        reject(new Error("Reclaim failed, task aborted!"));
      } else {
        // Otherwise we completed the task
        accept(exitcode);
      }
    });

  })).then(function(exitcode) {
    // First upload logs as artifacts
    return Promise.all(
      worker.putArtifact('stdout.log', 'stdout.log', 'plain/text'),
      worker.putArtifact('stderr.log', 'stderr.log', 'plain/text')
    ).spread(function(stdoutUrl, stderrUrl) {
      // Then upload logs and result
      return Promise.all(
        worker.putLogs({

        }),
        worker.putResult({
          version:            '0.2.0',
          artifacts: {
            "stdout.log":     stdoutUrl,
            "stderr.log":     stderrUrl
          },
          statistics: {
            started:          started.toJSON(),
            finished:         finished.toJSON()
          },
          worker: {
            workerGroup:      worker.options.workerGroup,
            workerId:         worker.options.workerId
          },
          // This is tasks specific results
          result: {
            exitcode:         exitcode
          }
        })
      );
    });
  }).then(function() {
    return worker.taskCompleted();
  });
};


/**
 * Start a task processing look, return a promise that fails, when the loop
 * ends due to missing tasks or errors
 */
var processLoop = function(worker) {
  // Number of failures allowed before the worker should exit
  var failures_allowed = 5;

  // Number of failures left before the worker should exit
  var failures_left = failures_allowed;

  // Promise that loop will event eventually
  return new Promise(function(accept, reject) {
    var iterate = function() {
      processTask(worker).then(function() {
        debug('Successfully processed a task');
        failures_left = failures_allowed;
        iterate();
      }, function(err) {
        debug('Failed to process task, error: %s, as JSON: %j', err, err);
        failures_left -= 1;
        if (failures_left > 0) {
          iterate();
        } else {
          reject();
        }
      });
    };
  });
};

program
  .command('start')
  .description("Create a simple AWS worker")
  .option('--provisioner-id',   "provisionerId, defaults 'aws-provisioner'")
  .option('--worker-type',      "workerType, defaults to instance-type + AMI image")
  .option('--worker-group',     "workerGroup, defaults to availability zone")
  .option('--worker-id',        "workerId, defaults to instance id")
  .option('-s, --shutdown', "Shutdown the machine when this process ends")
  .action(function(options) {
    // Provide default provisionerId
    if (!options.provisionerId) {
      options.provisionerId = 'aws-provisioner';
    }

    // Provide default workerType
    if (!options.workerType) {
      options.workerType = Promise.all(
        metadata.getInstanceType();
        metadata.getImageId();
      ).spread(function(instanceType, imageId) {
        return instanceType.replace('.', '-') + '_' + imageId;
      });
    }

    // Provide default workerGroup
    if (!options.workerGroup) {
      options.workerGroup = metadata.getAvailabilityZone();
    }

    // Provide default workerId
    if (!options.workerId) {
      options.workerId = metadata.getInstanceId();
    }

    // Create worker
    var worker_created = Promise.all(
      options.provisionerId,
      options.workerType,
      options.workerGroup,
      options.workerId
    ).spread(function(provisionerId, workerType, workerGroup, workerId) {
      return Worker({
        provisionerId:    'jonasfj-test-provisioner',
        workerType:       'jonasfj-test-worker-type',
        workerGroup:      'jonasfj-test-group',
        workerId:         'jonasfj-test-worker'
      });
    });

    // When worker is created, it's time to start the process loop
    worker_created.then(function(worker) {
      // wait for process loop to break and exit and possibly shutdown
      processLoop(worker).then(undefined, function() {
        spawn('sudo', ['shutdown', '-h', 'now']);
        process.exit(1);
      });
    });
  });
