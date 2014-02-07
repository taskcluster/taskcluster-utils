var program = require('commander');
var cliff   = require('cliff');
var utils   = require('./utils');

program
  .command('state')
  .description("Print current state from state.json")
  .action(function(options) {

  // Load state
  var state = utils.loadState();
  console.log(cliff.inspect(state));
});