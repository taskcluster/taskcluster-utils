var program = require('commander');
var fs      = require('fs');
var request = require('superagent');
var cliff   = require('cliff');
var nconf   = require('nconf');
var utils   = require('./utils');

program
  .command('list-tasks <task-state>')
  .description("Lists tasks available for the configured provisioner-id")
  .option('-j, --json', "Output results as JSON")
  .action(function(task_state, options) {

  // Check if we support the task_state requested
  var support_task_states = ['pending'];
  if (support_task_states.indexOf(task_state) == -1) {
    console.log("Sorry, the selected task state is not supported.".red);
    return;
  }

  // Load state from state.json
  var state = utils.loadState();

  // Fetch list of task statues from server
  request
    .get(utils.queueUrl('/pending-tasks/' + state.provisionerId))
    .end(function(res) {
      if (res.ok) {
        if (options.json) {
          console.log(cliff.inspect(res.body));
        } else {
          console.log(cliff.stringifyObjectRows(res.body.tasks, [
            'taskId', 'state', 'workerType', 'routing'
          ]));
        }
      } else {
        console.log("Failed to list pending tasks, errors:".bold);
        console.log(cliff.inspect(res.body));
      }
    });
});