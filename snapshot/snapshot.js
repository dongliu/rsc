var ca = require(__dirname + '/lib/ca.js'),
  fs = require('fs'),
  pv_list,
  size,
  results = {},
  complete = 0,
  start = Date.now();

if (process.argv.length == 2) {
  pv_list = JSON.parse(fs.readFileSync(__dirname + '/config/pv_list.json'));
} else if (process.argv.length == 3) {
  if (process.argv[2] === 'clean') {
    pv_list = JSON.parse(fs.readFileSync(__dirname + '/config/pv_list_clean.json'));
  } else {
    pv_list = JSON.parse(fs.readFileSync(__dirname + '/config/pv_list.json'));
  }
} else {
  console.warn('too many arguments.');
  return;
}

size = pv_list.length;

pv_list.forEach(function(pv) {
  ca.exec('caget', pv, function(err, result) {
    complete = complete + 1;
    if (err) {
      results[pv] = {
        name: pv,
        value: 'unavailable'
      };
    } else {
      results[pv] = ca.parseCaget(result);
    }
    if (complete == size) {
      console.log('the snapshot took ' + (Date.now() - start) + ' milliseconds.');
    }
  });
});



