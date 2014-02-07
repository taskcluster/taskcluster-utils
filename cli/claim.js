var program = require('commander');
var fs      = require('fs');
var request = require('superagent');
var cliff   = require('cliff');
var nconf   = require('nconf');
var utils   = require('./utils');

var baseUrl = 'http://' + nconf.get('queue:hostname') + ':' +
              nconf.get('queue:port');

program
  .command('claim [task-id]')
  .description("Claim or reclaim a task, this store the task-id to state.json")
  .action(function(taskId, options) {

  // Load state, we'll need this for the request
  var state = utils.loadState();

  // If taskId isn't provided this is a reclaim
  var run_id = undefined;
  if (taskId === undefined) {
    taskId = state.taskId;
    run_id = state.runId;
  }

  // Fetch task from S3
  request
    .post(baseUrl + '/0.2.0/task/' + taskId + '/claim')
    .send({
      worker_group:     state.workerGroup,
      worker_id:        state.workerId,
      run_id:           run_id
    })
    .end(function(res) {
      if (res.ok) {
        console.log("Task claimed until: " + res.body.status.taken_until.bold);
        console.log(cliff.inspect(res.body));
        state.runId     = res.body.status.run_id;
        state.taskId    = res.body.status.task_id;
        state.logsUrl   = res.body.logs_url;
        state.resultUrl = res.body.result_url;
        utils.saveState(state);
      } else {
        console.log("Failed to claim task, errors:".bold);
        console.log(cliff.inspect(res.body));
      }
    });
});