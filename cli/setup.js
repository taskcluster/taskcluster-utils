var fs      = require('fs');
var program = require('commander');
var prompt  = require('prompt');
var Promise = require('promise');
var cliff   = require('cliff');
var utils   = require('./utils')

prompt.delimiter = ' ';

program
  .command('setup')
  .description("Setup state.json so we can emulate components")
  .option('--provisioner-id [id]',  "Provisioner-id to setup")
  .option('--worker-type [id]',     "Worker-type to setup")
  .option('--worker-group [id]',    "Worker-group to setup")
  .option('--worker-id [id]',       "Worker-id to setup")
  .option('-y, --overwrite',        "Overwrite state.json if exists")
  .action(function(options) {
  // Prompt for missing properties
  prompt.start();

  // Promise that we can proceed to overwrite state.json
  var can_proceed = Promise.from(true);

  // Message for asking if we can delete state.json
  prompt.message = "Can we overwrite";

  // Check if state.json already exists
  if(fs.existsSync('state.json') && !options.overwrite) {
    can_proceed = new Promise(function(accept, reject) {
      console.log("Warning: ".bold + "state.json exists");
      prompt.get(['state.json [y/n]'], function(err, result) {
        if (err) {
          reject(err);
        } else {
          accept(result['state.json [y/n]'] == 'y');
        }
      });
    });
  }

  // When if ever we can proceed do this:
  can_proceed.then(function(overwrite) {
    // If we can't overwrite just stop here
    if (!overwrite) {
      return;
    }

    // Set a prompt message for asking sane questions
    prompt.message = "Enter";


    // Okay, let's create state.json
    var state = {
      provisionerId:  options.provisionerId,
      workerType:     options.workerType,
      workerGroup:    options.workerGroup,
      workerId:       options.workerId,
      runId:          null,
      taskId:         null,
      logsUrl:        null,
      resultUrl:      null
    };

    // Properties that we absolutely need
    var required_properties = [
      'provisionerId',
      'workerType',
      'workerGroup',
      'workerId'
    ];

    // Properties to prompt for
    var prompt_properties = required_properties.filter(function(prop) {
      return state[prop] === undefined;
    });

    // Create promise for state to be created
    var state_created = Promise.from(state);

    // If there is any properties to prompt for create a promise to prompt for
    // them
    if (prompt_properties.length > 0) {
      state_created = new Promise(function(accept, reject) {
        prompt.get(prompt_properties, function(err, result) {
          if (err) {
            reject(err);
          } else {
            prompt_properties.forEach(function(prop) {
              state[prop] = result[prop];
            });
            accept(state);
          }
        });
      });
    }

    // When state is created we should save to disk in current working folder
    state_created.then(function(state) {
      // Write to state.json
      utils.saveState(state);

      console.log("Wrote " + "state.json".bold + " to current working directory");
      console.log(cliff.inspect(state));
    });
  });
});