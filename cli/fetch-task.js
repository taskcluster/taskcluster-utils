var program = require('commander');
var fs      = require('fs');
var request = require('superagent');
var cliff   = require('cliff');
var nconf   = require('nconf');
var utils   = require('./utils');

program
  .command('fetch-task [task-id]')
  .description("Fetch task definition")
  .option('-d, --dump [FILE]', "Write task definition to file")
  .action(function(taskId, options) {

  // Load state if task-id isn't provided
  if (taskId === undefined) {
    var state = utils.loadState();
    taskId = state.taskId;
  }

  // Fetch task from S3
  request
    .get('http://tasks.taskcluster.net/' + taskId + '/task.json')
    .end(function(res) {
      if (res.ok) {
        console.log(cliff.inspect(res.body));
        if (options.dump) {
          fs.writeFileSync(options.dump, JSON.stringify(res.body, null, 4), {
            options:  'utf-8'
          });
        }
      } else {
        console.log("Failed to fetch task, errors:".bold);
        console.log(cliff.inspect(res.body));
      }
    });
});