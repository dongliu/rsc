'use strict';
var cluster = require('cluster');
var pattern = /GET\s\/ongoing\/When\/\d\d\dx\/(\d\d\d\d\/\d\d\/\d\d\/[^\s\.]+)\s/;
var numCPUs = require('os').cpus().length;

var fs = require('fs');
var stream = require('stream').Stream;
var size = fs.statSync('./O.Big.log').size;
var fileName = './O.Big.log';

var i, workerId, finished = 0;

var ends = [], starts, map = [], result = {}, resultArray = [], n = numCPUs - 1;

var now = Date.now();
if (cluster.isMaster) {
  for (i = 0; i < n; i += 1) {
    cluster.fork();
  }

  cluster.on('exit', function(worker, code, signal) {
    console.log('worker ' + worker.process.pid + ' died');
  });

  for (workerId in cluster.workers) {
    cluster.workers[workerId].on('message', function(reduce) {
      var k;
      finished += 1;
      if (reduce.result) {
        // console.log(reduce.result);
        for (k in reduce.result) {
          if (result.hasOwnProperty(k)) {
            result[k] += reduce.result[k];
          } else {
            result[k] = reduce.result[k];
          }
        }
      }
      if (finished === n) {
        for (k in result) {
          resultArray.push({key: k, value: result[k]});
        }
        resultArray.sort(function(a, b) {
          return (b.value - a.value);
        });
        for (i = 0; i < 10; i += 1) {
          console.log(resultArray[i] );
        }
        console.log('it took ' + (Date.now()-now));
        // destroy the workers
        Object.keys(cluster.workers).forEach(function(id) {
          cluster.workers[id].destroy();
        });
      }
    });
  }

  for (i = 0 ; i < n - 1; i += 1) {
    findEnd('./O.Big.log', Math.round(size * (i+1) / n), size, function(err, end) {
      var j, id;
      if (err) {
        console.log(err.message);
      } else {
        ends.push(end);
      }
      if (ends.length === n-1) {
        ends = ends.sort(function(a,b) {
          return (a - b);
        });
        starts = ends.slice(0);
        for (j = 0; j < n-1; j += 1) {
          starts[j] += 1;
        }
        ends.push(size);
        starts.unshift(0);
        j = 0;
        for (id in cluster.workers) {
          cluster.workers[id].send({range:{start: starts[j], end: ends[j]}});
          j += 1;
        }
      }
    });
  }
} else if(cluster.isWorker) {
  process.on('message', function(msg) {
    if (msg.range) {
      finder(fileName, msg.range.start, msg.range.end, pattern, function(err, result) {
        if (err) {
          process.send({err: err});
        } else {
          process.send({result: result});
        }
      });
    }
  });

}

function finder(fileName, start, end, pattern, cb) {
  var fs = require('fs');
  var StringDecoder = require('string_decoder').StringDecoder;
  var decoder = new StringDecoder('utf8');
  var result = {};
  var stream = fs.createReadStream(fileName, {
      start: start,
      end: end
  });
  var newline = '\n'.charCodeAt(0);
  var linenumber = 0;
  var left = new Buffer(0);
  stream.on('data', function(data) {
    var i, last = 0, line, match;
    for (i = 0; i < data.length; i += 1) {
      if(newline === data[i]) {
        linenumber += 1;
        if (last === 0 && Buffer.isBuffer(left) && left.length !== 0) {
          line = Buffer.concat([left, data.slice(0, i)]).toString('utf8');
          left = new Buffer(0);
        } else {
          line = data.toString('utf8', last, i);
        }
        match = pattern.exec(line);
        if (match) {
          // console.log(match[1]);
          if (result[match[1]]) {
            result[match[1]] += 1;
          } else {
            result[match[1]] = 1;
          }
        }
        last = i + 1;
      }
    }
    left = Buffer.concat([left, data.slice(last)]);
  });
  stream.on('end', function(){
    console.log('processed ' + linenumber + ' lines');
    cb(null,result);
  });
  stream.on('error', function(err){
    cb(err, null);
  });

}

function findEnd(fileName, position, size, cb) {
  var fs, stream, last = position;
  if (position >= size) {
    return cb(null, size);
  }
  fs = require('fs');
  stream = fs.createReadStream(fileName, {
    start: position
  });
  stream.on('data', function(data) {
    var newline = '\n'.charCodeAt(0);
    // var i, found = false, last = position;
    var i;
    for (i = 0; i < data.length; i += 1) {
      if(newline === data[i]) {
        // find an ending
        stream.destroy();
        return cb(null, last + i);
      }
    }
    last += i;
  });
  stream.on('end', function(){
    return cb(new Error('not found any ending in the file'), null);
  });
  stream.on('error', function(err){
    return cb(err, null);
  });
}
