var config  = require('../config');
var program = require('commander');

config.load(true);

program
  .version('0.2.0');
  // TODO: load configuration files

// Program is define the following submodules, which when loaded constructs
// the commandline interface
[
  './setup',
  './post-task',
  './list-tasks',
  './fetch-task',
  './claim',
  './state',
  './put-artifact'
].forEach(function(module) {
  require(module);
});

// Export program
exports.program = program;
