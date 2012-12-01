var fileName = process.argv[2];
var fs = require('fs');
var bufferSize = 64 * 1024 * 1024;
var size = fs.statSync(fileName).size;

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

function readThrough(fd, size, bufferSize, position, prefix, process, u_hits, u_bytes, s404s, clients, refs) {
  fs.read(fd, new Buffer(bufferSize), 0, bufferSize, position, function(err, bytesRead, buffer){
    var lines = buffer.toString('utf8').split(/\n/);
    if(prefix.length > 0) {
      lines[0] = prefix + lines[0];
    }
    var last = lines.pop();
    process(lines, u_hits, u_bytes, s404s, clients, refs);
    if (position + bufferSize >= size) {
      console.log('file finished');
      console.log('URIs by hit');
      sortObject(u_hits);
      console.log('URIs by bytes');
      sortObject(u_bytes);
      console.log('404s');
      sortObject(s404s);
      console.log('client addresses');
      sortObject(clients);
      console.log('referrers');
      sortObject(refs);
      return console.log('done');
    } else {
      readThrough(fd, size, bufferSize, (position + bytesRead + 1), last, process, u_hits, u_bytes, s404s, clients, refs);
    }
  });
}

fs.open(fileName,'r', function(err, fd) {
  if (err) {
    return console.log(err.message);
  }
  readThrough(fd, size, bufferSize, 0, '', parse, {}, {}, {}, {}, {});
});