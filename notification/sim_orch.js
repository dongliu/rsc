var fs = require('fs'), 
  sim = require('./sim'),
  network_event,
  simulator,
  // config,
	config = JSON.parse(fs.readFileSync(__dirname + '/network_3_1.json')),
	job_size = 1000,
  current, 
  network_state, 
  event_source = [],
  event_name,
  arrive_time, i, j, waiting,
  fail,
  stats = []; // all stats here

// if (process.argv.length === 4) {
//   config = JSON.parse(fs.readFileSync(__dirname + '/' + process.argv[2]));
//   job_size = parseInt(process.argv[3], 10);
// } else {
//   console.warn('wrong number of arguments.');
//   return;
// }

console.log(config);
console.log(job_size);

// init
current = 0;
fail = 0;
success = 0;
network_state = [1, 1];
// event_source = new Array(2+config.partition);
event_source.push(nextExp(config.rate));
// event_source.push(event_source[0]+config.timeout);
event_source.push(Math.random()*nextGaussian(config.mttf_mu, config.sigma));
event_source.push(Math.random()*nextGaussian(config.mttf_mu, config.sigma));
event_name = ['arrival', '1-2', '1-3'];
waiting = new Array(config.partition);
for (i = 0; i < config.partition; i += 1) {
  waiting[i] = [];
}

// console.log(event_source);

simulator = new sim.Sim;
network_event = new sim.Sim;
// arrival_event = new sim.Sim;

// sim_event.on('arrival', function(current){
network_event.on('arrival', function(){
  // console.log('arrival at ' + current);
  job_size = job_size - 1;
  // current = time;
  for (var j = 0; j < config.service; j += 1) {
    // stats.push(nextBoundedPareto(config.min/10, config.max/10, config.shape));
    success += 1;
  }
  for(var i = 0; i < config.partition; i += 1) {
    if (network_state[i] === 1) {
      for (j = 0; j < config.service; j += 1) {
        // stats.push(nextBoundedPareto(config.min, config.max, config.shape));
        success += 1;
      }
    } else {
      waiting[i].push(current+config.timeout);
    }
  }
  event_source[0] = current + nextExp(config.rate);
  process.nextTick(simulator.emit('next'));
  // simulator.emit('next', 'arrival');
});

// sim_event.on('1-2', function(current){
network_event.on('1-2', function(){
  // console.log(network_state);
  // console.log('connection 1-2 changes at ' + current);
  if (network_state[0] === 1) {
    network_state[0] = 0;
    event_source[1] = current + nextGaussian(config.mttr_mu, config.sigma);
  } else {
    // console.log(waiting);
    network_state[0] = 1;
    for (var i = 0; i < waiting[0].length; i += 1) {
      if (current < waiting[0][i]) {
        break;
      }
    }
    // console.log('failed ' + i);
    fail += i * config.service;
    for (var j = 0; j < waiting[0].length - i; j += 1) {
      for (var k = 0; k < config.service; k += 1) {
        // stats.push(current - waiting[0][j] + config.timeout + nextPareto(config.min, config.shape));
        success += 1;
      } 
    }
    waiting[0] = []; // clear waiting list
    event_source[1] = current + nextGaussian(config.mttf_mu, config.sigma);
  }
  process.nextTick(simulator.emit('next'));
  // simulator.emit('next', '1-2');
});

// sim_event.on('1-3', function(current){
network_event.on('1-3', function(){
  // console.log(network_state);
  // console.log('connection 1-3 changes at ' + current);
  if (network_state[1] === 1) {
    network_state[1] = 0;
    event_source[2] = current + nextGaussian(config.mttr_mu, config.sigma);
  } else {
    // console.log(waiting);
    network_state[1] = 1;
    for (var i = 0; i < waiting[1].length; i += 1) {
      if (current < waiting[1][i]) {
        break;
      }
    }
    // console.log('failed ' + i);
    fail += i * config.service;
    for (var j = 0; j < waiting[1].length - i; j += 1) {
      for (var k = 0; k < config.service; k += 1) {
        // stats.push(current - waiting[1][j] + config.timeout + nextPareto(config.min, config.shape));
        success += 1;
      } 
    }
    waiting[1] = []; // clear waiting list
    event_source[2] = current + nextGaussian(config.mttf_mu, config.sigma);
  }
  process.nextTick(simulator.emit('next'));
  // simulator.emit('next', '1-3')
});

simulator.on('next', function(){
  // console.log('next');
  var min = minimum(event_source);
  // console.log(event_source);
  // console.log(min);
  var i; 
  for (i = 0; i < event_source.length; i += 1) {
    if (min === event_source[i]) {
      if (i === 0 && job_size <= 0) { // jobs finished
        console.log('done');
        console.log('fail: ' + fail);
        console.log('success: ' + success);
        // console.log(event_source);
        // console.log(waiting);
        console.log('stats: ' + stats.length);
        // console.log('stats: ' + stats);
        simulator.removeAllListeners();
        network_event.removeAllListeners();

        process.exit(0);
      } else {
        // process.nextTick(sim_event.emit(event_name[i], min));
        // console.log('current is ' + min);
        // sim_event.emit(event_name[i], min);
        current = min;
        // switch (i) {
        //   case 0: {
        //     sim_event.emit('arrival');
        //     break;
        //   }
        //   case 1: {
        //     sim_event.emit('1-2');
        //     break;
        //   }
        //   case 2: {
        //     sim_event.emit('1-3');
        //     break;
        //   }

        // }
        // network_event.emit(event_name[i], 'next');
        process.nextTick(network_event.emit(event_name[i]));
      }
        
    }
  }
});


simulator.emit('next');


function minimum(array) { 
  if (array.length === 0) {
    return null;
  }
  if (array.length === 1) {
    return array[0];
  }
  var i;
  var min = array[0];
  for (i = 1; i < array.length; i += 1) {
    if (min > array[i]) {
      min = array[i];
    }
  }
  return min;
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
    var l = 1, h = Math.pow(1+max-min, shape), rnd = Math.random();
    while (rnd === 0) { rnd = Math.random(); }
    return Math.pow((rnd*(h-l)-h) / -(h*l), -1/shape)-1+min;
}

