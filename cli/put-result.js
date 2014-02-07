var program = require('commander');
var fs      = require('fs');
var request = require('superagent');
var cliff   = require('cliff');
var nconf   = require('nconf');
var utils   = require('./utils');

program
  .command('put-result <result.json>')
  .description("Put result.json to URL from state.json")
  .action(function(file, options) {

  // Load state, we'll need this for the request
  var state = utils.loadState();

  // Test that specified file exists
  var stat = fs.statSync(file);
  if (!stat.isFile()) {
    console.log(("No such file: " + file.bold).red);
    process.exit(1);
  }

  var req = request
              .put(state.resultUrl)
              .set('Content-Type',    'application/json')
              .set('Content-Length',  stat.size);
  fs.createReadStream(file).pipe(req, {end: false});
  req.end(function(res) {
    console.log(res.status);
    if (res.ok) {
      console.log("Uploaded result.json successful".bold);
    } else {
      console.log("Upload to signed url failed".bold.red);
    }
  });
});