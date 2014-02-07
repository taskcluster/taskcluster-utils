var program = require('commander');
var fs      = require('fs');
var request = require('superagent');
var cliff   = require('cliff');
var nconf   = require('nconf');
var utils   = require('./utils');

var baseUrl = 'http://' + nconf.get('queue:hostname') + ':' +
              nconf.get('queue:port');

program
  .command('complete')
  .description("Report task from state.json as completed")
  .action(function(options) {

  // Load state, we'll need this for the request
  var state = utils.loadState();

  // Fetch task from S3
  request
    .post(baseUrl + '/0.2.0/task/' + state.taskId + '/completed')
    .send({
      worker_group:     state.workerGroup,
      worker_id:        state.workerId,
      run_id:           state.runId
    })
    .end(function(res) {
      if (res.ok) {
        console.log("Task " + state.taskId.bold + " completed!");
        console.log(cliff.inspect(res.body));
      } else {
        console.log("Failed to report task completed, errors:".bold.red);
        console.log(cliff.inspect(res.body));
      }
    });
});