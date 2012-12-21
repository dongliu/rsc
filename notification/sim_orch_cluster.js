var fs = require('fs'), 
  cluster = require('cluster'),
  worker,
  config,
	// config = JSON.parse(fs.readFileSync(__dirname + '/network_3_1.json')),
  // job_size = 10000,
	job_size,
  network_state, 
  event_source = [],
  event_name,
  waiting,
  fail,
  stats = []; // all stats here

if (process.argv.length === 4) {
  config = JSON.parse(fs.readFileSync(__dirname + '/' + process.argv[2]));
  job_size = parseInt(process.argv[3], 10);
} else {
  console.warn('wrong number of arguments.');
  return;
}


if (cluster.isMaster){
  // init cluster
  event_source.push(nextExp(config.rate));
  event_source.push(Math.random()*nextGaussian(config.mttf_mu, config.sigma));
  event_source.push(Math.random()*nextGaussian(config.mttf_mu, config.sigma));
  event_name = ['arrival', '1-2', '1-3'];
  console.log(config);
  console.log(job_size);
  worker = cluster.fork();
  cluster.on('exit', function(worker, code, signal) {
    console.log('worker ' + worker.process.pid + ' died');
  });
  worker.on('message', function(msg) {
    // console.log(msg);
    event_source[msg.type] = msg.update;
    var index = minIndex(event_source);
    if (job_size <= 0 && index === 0) { // jobs finished
      console.log('done');
      worker.send({'type': 'done'});
    } else {
      if (index === 0) {
        job_size -= 1;
      }
      worker.send({'type': event_name[index], 'current': event_source[index]});
    }

  });
  // start the simulation 
  var start = minIndex(event_source);
  job_size -= 1;
  worker.send({'type': event_name[start], 'current': event_source[start]});
} else if (cluster.isWorker) {
  // init worker
  fail = 0;
  success = 0;
  network_state = [1, 1];
  waiting = new Array(config.partition);
  for (i = 0; i < config.partition; i += 1) {
    waiting[i] = [];
  }
  process.on('message', function(msg){
    // console.log(msg);
    var update;
    switch (msg.type) {
      case 'arrival': {
        update = updateArrival(msg.current);
        process.send({'type': 0, 'update': update});
        break;
      }
      case '1-2': {
        update = updateNetwork(msg.current, 0);
        process.send({'type': 1, 'update': update});
        break;
      }
      case '1-3': {
        update = updateNetwork(msg.current, 1);
        process.send({'type': 2, 'update': update});
        break;
      }
      case 'done': {
        console.log('fail: ' + fail);
        console.log('success: ' + success);
        // console.log('average: ' + sum(stats)/stats.length );
        console.log('average: ' + average(stats) );
        // console.log(stats);
        process.exit(0);
        break;
      }
    }
  });
}

function updateArrival(current) {
  for (var j = 0; j < config.service; j += 1) {
    stats.push(nextBoundedPareto(config.min/10, config.max/10, config.shape));
    success += 1;
  }
  for(var i = 0; i < config.partition; i += 1) {
    if (network_state[i] === 1) {
      for (j = 0; j < config.service; j += 1) {
        stats.push(nextBoundedPareto(config.min, config.max, config.shape));
        success += 1;
      }
    } else {
      waiting[i].push(current+config.timeout);
    }
  }
  return current + nextExp(config.rate);
}

function updateNetwork(current, interface) {
  // console.log('network: ' + network_state + ' current: ' + current + ' interface: ' + interface);
  if (network_state[interface] === 1) {
    network_state[interface] = 0;
    return current + nextGaussian(config.mttr_mu, config.sigma);
  } else {
    // console.log(waiting);
    network_state[interface] = 1;
    for (var i = 0; i < waiting[interface].length; i += 1) {
      if (current < waiting[interface][i]) {
        break;
      }
    }
    // console.log('failed ' + i);
    fail += i * config.service;
    for (var j = 0; j < waiting[interface].length - i; j += 1) {
      for (var k = 0; k < config.service; k += 1) {
        stats.push(current - waiting[interface][j] + config.timeout + nextBoundedPareto(config.min, config.max, config.shape));
        success += 1;
      } 
    }
    waiting[interface] = []; // clear waiting list
    return current + nextGaussian(config.mttf_mu, config.sigma);
  }
}

function minIndex(array) { 
  if (array.length === 0) {
    return null;
  }
  if (array.length === 1) {
    return 0;
  }
  var i;
  var min = array[0], position = 0;
  for (i = 1; i < array.length; i += 1) {
    if (min > array[i]) {
      min = array[i];
      position = i;
    }
  }
  return position;
}

function average(input) {
  return input.reduce(function(p, c, i, a) {
    return (p * i + c) / (i + 1); 
  });
}

function sum(input) {
  var s = 0; 
  for (var i = 0; i < input.length; i += 1) {
    s += input[i];
  }
  return s;
}

function nextExp(rate) {
  var rnd = Math.random();
  while (rnd === 0) { 
    rnd = Math.random(); 
  }
  return -1 * Math.log(rnd)/rate;
}


function nextGaussian(mean, stddev) {
  mean = mean || 0;
  stddev = stddev || 1;
  var s = 0, z0, z1;
  while (s === 0 || s >= 1) {
    z0 = 2 * Math.random() - 1;
    z1 = 2 * Math.random() - 1;
    s = z0*z0 + z1*z1;
  }
  return z0 * Math.sqrt(-2 * Math.log(s) / s) * stddev + mean;
}

function nextPareto(min, shape) {
  shape = shape || 3;
  var rnd = Math.random();
  return Math.pow(1 - rnd, -1/shape) * min;
}

function nextBoundedPareto(min, max, shape) {
  shape = shape || 0.1;
  var l = 1, rnd = Math.random();
  while (rnd === 0) { 
    rnd = Math.random(); 
  }
  return min*max/Math.pow((Math.pow(max, shape)*(1-rnd) + Math.pow(min, shape)*rnd), 1/shape);
  // var p = Math.pow(Math.pow(min, shape)/(rnd*Math.pow(min/max, shape)-rnd+1), 1/shape);
  // if (isNaN(p)) {
  //   console.
  //   return max;
  // } else {
  //   return p;
  // }
}

