var cluster = require('cluster');
var util = require('util');
var numCPUs = require('os').cpus().length;
var fs = require('fs');
var fileName = process.argv[2];
var size = fs.statSync(fileName).size;
var blockSize = 64*1024*1024;
var i, reduced = 0, jobSize = 0;
var reduce_u_hits = {}, reduce_u_bytes = {}, reduce_s404s = {}, reduce_clients = {}, reduce_refs = {};
var jobs = [];

if (cluster.isMaster) {
  for (i = 0; i < numCPUs -1; i += 1) {
    cluster.fork();
  }

  cluster.on('exit', function(worker, code, signal) {
    console.log('worker ' + worker.process.pid + ' died');
  });

  Object.keys(cluster.workers).forEach(function(workerId) {
    cluster.workers[workerId].on('message', function(msg) {
      var k, result, wid, now;
      reduced += 1;
      if (util.isError(msg)) {
        console.log(reduce.message);
      } else {
        if (jobs.length > 0) {
          cluster.workers[msg.workerId].send({workerId: msg.workerId, job: jobs.pop()});
        }
        result = msg.result;
        for (k in result.u_hits) {
          updateObject(reduce_u_hits, k, result.u_hits[k]);
        }
        for (k in result.u_bytes) {
          updateObject(reduce_u_bytes, k, result.u_bytes[k]);
        }
        for (k in result.s404s) {
          updateObject(reduce_s404s, k, result.s404s[k]);
        }
        for (k in result.clients) {
          updateObject(reduce_clients, k, result.clients[k]);
        }
        for (k in result.refs) {
          updateObject(reduce_refs, k, result.refs[k]);
        }

        if (reduced === jobSize) {
          now = Date.now();
          console.log('URIs by hit');
          sortObject(reduce_u_hits);
          console.log('URIs by bytes');
          sortObject(reduce_u_bytes);
          console.log('404s');
          sortObject(reduce_s404s);
          console.log('client addresses');
          sortObject(reduce_clients);
          console.log('referrers');
          sortObject(reduce_refs);
          console.log('sorting takes ' + (Date.now() - now));
          Object.keys(cluster.workers).forEach(function(wid) {
            cluster.workers[wid].destroy();
          });
          process.exit(0);
        }
      }
    });
  });

  findEnds(fileName, 0, blockSize, size, [], function(err, ends) {
    var starts, i;
    if (err) {
      console.log(err.message);
      process.exit(0);
    } else {
      starts = ends.slice(0, -1);
      for (i = 0; i < starts.length; i += 1) {
        starts[i] += 1;
      }
      starts.unshift(0);
      for (i = 0; i < starts.length; i += 1) {
        jobs.push({start: starts[i], end: ends[i]});
      }
      jobSize = jobs.length;
      for (workerId in cluster.workers) {
        if (jobs.length > 0) {
          cluster.workers[workerId].send({workerId: workerId, job: jobs.pop()});
        }
      }
    }
  });


} else if(cluster.isWorker) {
  process.on('message', function(msg) {
    var fs = require('fs'), stream, block = '';
    var u_hits = {}, u_bytes = {}, s404s = {}, clients = {}, refs = {}, lines;
    if (msg.job.start !== null && msg.job.end !== null) {
      fs.open(fileName,'r', function(err, fd) {
        if (err) {
          process.send(err);
        } else {
          stream = fs.createReadStream(fileName, {
            flags: 'r',
            encoding: 'utf8',
            start: msg.job.start,
            end: msg.job.end
          });
          stream.on('data', function(data) {
            block = block + data;
          });
          stream.on('end', function() {
            lines = block.split(/\n/);
            if (lines[lines.length-1].length === 0 ) {
              lines.pop();
            }
            parse(lines, u_hits, u_bytes, s404s, clients, refs);
            process.send({workerId: msg.workerId, result: {u_hits: u_hits, u_bytes: u_bytes, s404s: s404s, clients: clients, refs: refs}});
          });
        }
      });
    }
  });

}

function sortObject(obj) {
  var array = [], k, i;
  for (k in obj) {
    array.push({key: k, value: obj[k]});
  }
  array.sort(function(a, b) {
    return (b.value - a.value);
  });
  for (i = 0; i < 10; i += 1) {
    console.log('     ' + array[i].value + ':' + array[i].key.substring(0,59));
  }
}

function updateObject(obj, key, value) {
  if (obj[key]) {
    obj[key] += value;
  } else {
    obj[key] = value;
  }
}

function findEnd(fileName, position, size, cb) {
  var fs, stream, last = position;
  if (position >= size) {
    return cb(null, size-1);
  }
  fs = require('fs');
  stream = fs.createReadStream(fileName, {
    start: position
  });
  stream.on('data', function(data) {
    var newline = '\n'.charCodeAt(0);
    var i;
    for (i = 0; i < data.length; i += 1) {
      if(newline === data[i]) {
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

function findEnds(fileName, position, blockSize, size, ends, cb) {
  findEnd(fileName, position + blockSize, size, function(err, end) {
    if (err) {
      return cb(err, null);
    }
    ends.push(end);
    if (end === size - 1) {
      return cb(null, ends);
    } else {
      findEnds(fileName, end + 1, blockSize, size, ends, cb);
    }
  });
}

function parse(lines, u_hits, u_bytes, s404s, clients, refs) {
  var i, line, f, client, u, status, bytes, ref;
  var pattern = /^\/ongoing\/When\/\d\d\dx\/\d\d\d\d\/\d\d\/\d\d\/[^\s\.]+$/;
  var refPattern = /^"http:\/\/www\.tbray\.org\/ongoing\//;
  for (i = 0; i < lines.length; i += 1) {
    line = lines[i];
    f = line.split(/\s/);
    if (f[5] === '"GET') {
      client = f[0];
      u = f[6];
      status = f[8];
      bytes = f[9];
      ref = f[10];
      if (status === '200') {
        updateObject(u_bytes, u, parseInt(bytes, 10));
        if (pattern.test(u)) {
          updateObject(u_hits, u, 1);
          updateObject(clients, client, 1);
          if (!(ref === '"-"' || refPattern.test(ref))) {
            updateObject(refs, ref.substring(1, ref.length-1), 1);
          }
        }
      } else if (status === '304') {
        if (pattern.test(u)) {
          updateObject(u_hits, u, 1);
          updateObject(clients, client, 1);
          if (!(ref === '"-"' || refPattern.test(ref))) {
            updateObject(refs, ref.substring(1, ref.length-1), 1);
          }
        }
      } else if (status === '404') {
        updateObject(s404s, u, 1);
      }
    }
  }
}