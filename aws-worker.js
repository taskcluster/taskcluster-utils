var program       = require('commander');
var Promise       = require('promise');
var fs            = require('fs');
var Worker        = require('./lib/worker');
var metadata      = require('./lib/aws-metadata');
var spawn         = require('child_process').spawn;
var debug         = require('debug')('aws-worker');
var config        = require('./config');

require('./utils/spread-promise').patch();

var claim_work_attempts = 5;
var claim_work_delay    = 30 * 1000;

/** Process a task with the worker */
var processTask = function(worker) {
  // Claim work
  var started = null;
  var finished = null;
  var retries = claim_work_attempts;
  var handler = function(claimedTask) {
    if(!claimedTask) {
      if (retries > 0) {
        retries--;
        debug("claimWork found no available tasks");
        return new Promise(function(accept, reject) {
          setTimeout(function() {
            worker.claimWork().then(accept, reject);
          }, claim_work_delay);
        }).then(handler);
      }
      throw new Error("No tasks available");
    }
    return true;
  };

  return worker.claimWork().then(handler).then(function() {
    return new Promise(function(accept, reject) {
      // Get task definition
      var task = worker.task();

      // Create some log files
      debug("Creating log files");
      var stdout = fs.openSync('stdout.log', 'w');
      var stderr = fs.openSync('stderr.log', 'w');

      // Set time at which task started
      started = new Date();

      // Start worker process
      debug("Starting task subprocess")
      var process = spawn(
        task.payload.command,
        task.payload.arguments,
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
        debug("Task subprocess exited %s", exitcode);

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
    });
  }).then(function(exitcode) {
    debug("Uploading log artifacts, exitcode: %s", exitcode)
    // First upload logs as artifacts
    return Promise.all(
      worker.putArtifact('stdout.log', 'stdout.log', 'plain/text'),
      worker.putArtifact('stderr.log', 'stderr.log', 'plain/text')
    ).spread(function(stdoutUrl, stderrUrl) {
      // Then upload logs and result
      return Promise.all(
        worker.putLogs({
          version:            '0.2.0',
          logs: {
            "stdout.log":     stdoutUrl,
            "stderr.log":     stderrUrl
          }
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
var processLoop = function(worker, allowed_failures) {
  debug("Starting task processing loop");

  // Number of failures left before the worker should exit
  var failures_left = allowed_failures;

  // Promise that loop will event eventually
  return new Promise(function(accept, reject) {
    var iterate = function() {
      debug("New task processing iteration");
      processTask(worker).then(function() {
        debug('Successfully processed a task');
        failures_left = allowed_failures;
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
    // Start first loop iteration
    iterate();
  });
};

program
  .command('start')
  .description("Create a simple AWS worker")
  .option(
    '--provisioner-id <provisioner-id>',
    "provisionerId, defaults 'aws-provisioner'"
  )
  .option(
    '--worker-type <worker-type>',
    "workerType, defaults to instance-type + AMI image"
  )
  .option(
    '--worker-group <worker-group>',
    "workerGroup, defaults to availability zone"
  )
  .option(
    '--worker-id <worker-id>',
    "workerId, defaults to instance id"
  )
  .option(
    '-s, --shutdown',
    "Shutdown the machine when this process ends"
  )
  .option(
    '-n, --allowed-failures <N>',
    "Number of failures before task processing loop is killed"
  )
  .action(function(options) {
    // Load configuration
    config.load();

    // If not specified, allow for 5 failures
    if (options.allowedFailures === undefined) {
      options.allowedFailures = 5;
    }

    // Provide default provisionerId
    if (options.provisionerId === undefined) {
      options.provisionerId = 'aws-provisioner';
    }

    // Provide default workerType
    if (options.workerType === undefined) {
      options.workerType = Promise.all(
        metadata.getInstanceType(),
        metadata.getImageId()
      ).spread(function(instanceType, imageId) {
        return instanceType.replace('.', '-') + '_' + imageId;
      });
    }

    // Provide default workerGroup
    if (options.workerGroup === undefined) {
      options.workerGroup = metadata.getAvailabilityZone();
    }

    // Provide default workerId
    if (options.workerId === undefined) {
      options.workerId = metadata.getInstanceId();
    }

    // Create worker
    var worker_created = Promise.all(
      options.provisionerId,
      options.workerType,
      options.workerGroup,
      options.workerId
    ).spread(function(provisionerId, workerType, workerGroup, workerId) {
      return new Worker({
        provisionerId:    provisionerId,
        workerType:       workerType,
        workerGroup:      workerGroup,
        workerId:         workerId
      });
    });

    // When worker is created, it's time to start the process loop
    worker_created.then(function(worker) {
      // wait for process loop to break and exit and possibly shutdown
      processLoop(worker, options.allowedFailures).then(undefined, function(err) {
        debug("Worker ended with error: %s, as JSON: %j", err, err);
        if (options.shutdown) {
          spawn('sudo', ['shutdown', '-h', 'now']);
        }
        process.exit(0);
      });
    }).then(undefined, function(err) {
      debug("Initialization failed, error: %s, as JSON: %j", err, err);
      process.exit(1);
    });
  });

// Run program with command line arguments
program.parse(process.argv);