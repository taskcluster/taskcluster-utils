var nconf   = require('nconf');

/**
 * Default configuration values used by this library
 *
 * It's poor practice to use global configuration state like nconf in a library
 * like this, but it works... and is very convenient at the moment, feel free
 * rewrite it something smarter later if you would like.
 */
var DEFAULT_CONFIG_VALUES = {

  // TaskCluster Queue configuration
  'queue': {
    // Server hostname
    'hostname':                     'localhost',

    // Port to run the HTTP server on
    'port':                         3000
  }
};

/** Load configuration */
exports.load = function(default_only) {

  if (!default_only) {
    // Load configuration from command line arguments, if requested
    nconf.argv();

    // Config from current working folder if present
    nconf.file('local', 'taskcluster-utils.conf.json');

    // User configuration
    nconf.file('user', '~/.taskcluster-utils.conf.json');

    // Global configuration
    nconf.file('global', '/etc/taskcluster-utils.conf.json');
  }

  // Load default configuration
  nconf.defaults(DEFAULT_CONFIG_VALUES);
}
