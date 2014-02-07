var fs    = require('fs');
var cliff = require('cliff')

/** Load current state.json */
exports.loadState = function() {
  try {
    var data = fs.readFileSync('state.json', {encoding: 'utf-8'});
    var state = JSON.parse(data);
    return state;
  }
  catch (err) {
    console.log(("Failed to load " + "state.json".bold +
                 " make sure you run setup").red);
    console.log(cliff.inspect(err));
    process.exit(1);
    return null;
  }
};

/** Load current state.json */
exports.saveState = function(state) {
  fs.writeFileSync('state.json', JSON.stringify(state), {encoding: 'utf-8'});
};

