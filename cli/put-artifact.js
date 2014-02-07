var program = require('commander');
var fs      = require('fs');
var request = require('superagent');
var cliff   = require('cliff');
var nconf   = require('nconf');
var mime    = require('mime')
var utils   = require('./utils');

var baseUrl = 'http://' + nconf.get('queue:hostname') + ':' +
              nconf.get('queue:port');

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
  artifacts[name] = contentType;

  // Fetch task from S3
  request
    .post(baseUrl + '/0.2.0/task/' + state.taskId + '/artifact-urls')
    .send({
      worker_group:     state.workerGroup,
      worker_id:        state.workerId,
      run_id:           state.runId,
      artifacts:        artifacts
    })
    .end(function(res) {
      if (res.ok) {
        console.log("Fetched signed S3 put URL from queue");
        var req = request
                    .put(res.body.artifact_urls[name])
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