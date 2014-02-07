var program = require('commander');
var fs      = require('fs');
var request = require('superagent');
var cliff   = require('cliff');
var nconf   = require('nconf');

var baseUrl = 'http://' + nconf.get('queue:hostname') + ':' +
              nconf.get('queue:port');

program
  .command('post-task <task.json>')
  .description("Post task to the queue")
  .action(function(task_json, options) {

  // Load task.json
  var data = fs.readFileSync(task_json, {encoding: 'utf-8'});
  var task = JSON.parse(data);

  // Post to server
  request
    .post(baseUrl + '/0.2.0/task/new')
    .send(task)
    .end(function(res) {
      if (res.ok) {
        var task_id = res.body.status.task_id;
        console.log("Task posted successfully, task-id: " + task_id.bold);
        console.log(cliff.inspect(res.body));
      } else {
        console.log("Failed to post task, errors:".bold);
        console.log(cliff.inspect(res.body));
      }
    });
});