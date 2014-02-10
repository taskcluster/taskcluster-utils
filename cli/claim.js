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
  var runId = undefined;
  if (taskId === undefined) {
    taskId = state.taskId;
    runId = state.runId;
  }

  // Fetch task from S3
  request
    .post(utils.queueUrl('/task/' + taskId + '/claim'))
    .send({
      workerGroup:      state.workerGroup,
      workerId:         state.workerId,
      runId:            runId
    })
    .end(function(res) {
      if (res.ok) {
        console.log("Task claimed until: " + res.body.status.takenUntil.bold);
        console.log(cliff.inspect(res.body));
        state.runId     = res.body.runId;
        state.taskId    = res.body.status.taskId;
        state.logsUrl   = res.body.logsPutUrl;
        state.resultUrl = res.body.resultPutUrl;
        utils.saveState(state);
      } else {
        console.log("Failed to claim task, errors:".bold.red);
        console.log(cliff.inspect(res.body));
      }
    });
});