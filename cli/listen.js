var program   = require('commander');
var cliff     = require('cliff');
var nconf     = require('nconf');
var amqp      = require('amqp');
var Promise   = require('promise');

program
  .command('listen <pending|running|completed|failed>')
  .description("Listen for messages")
  .option('-r, --routing-pattern', "Routing pattern to bind with, default: '#'")
  .action(function(source, options) {
    // List of valid sources
    var sources = [
      'pending', 'running', 'completed', 'failed'
    ];

    // Check if source is valid
    if (sources.indexOf(source) == -1) {
      console.log(("Invalid message source: " + source.bold).red);
      process.exit(1);
    }

    // Get routing pattern, defaults to '#'
    var routingPattern = options.routingPattern || '#';

    // Create a connection
    var conn = null;
    var connected = new Promise(function(accept, reject) {
      // Create connection
      conn = amqp.createConnection(nconf.get('amqp'));
      conn.on('ready', accept);
    });

    connected.then(function() {
      queue = conn.queue('', {
        passive:                    false,
        durable:                    false,
        exclusive:                  true,
        autoDelete:                 true,
        closeChannelOnUnsubscribe:  true
      }, function() {
        queue.subscribe(function(message) {
          console.log("--------- Received at " + new Date().toJSON().bold + " ---------");
          console.log(cliff.inspect(message));
        });
        queue.bind(
          'v1/queue:task-' + source,
          routingPattern,
          function() {
            console.log('Listening...');
          }
        );
      });
    });
  }
);