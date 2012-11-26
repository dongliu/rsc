
var pattern = /GET\s\/ongoing\/When\/\d\d\dx\/(\d\d\d\d\/\d\d\/\d\d\/[^\s\.]+)\s/;

function test_multiple_finder(n, pattern) {
  'use strict';
  var fs = require('fs');
  var assert = require('assert');
  var stream = require('stream').Stream;

  var size = fs.statSync('./O.Big.log').size;
  console.log('file size is ' + size);

  var now = Date.now();

  var ends = [], starts, map = [];

  var i = 0, result = {};

  for (i = 0 ; i < n-1; i += 1) {
    findEnd('./O.Big.log', Math.round(size * (i+1) / n), size, function(err, end) {
      var j;
      if (err) {
        console.log(err.message);
      } else {
        // console.log(end);
        ends.push(end);
      }
      if (ends.length === n-1) {
        // console.log(ends);
        ends = ends.sort(function(a,b) {
          return (a - b);
        });
        starts = ends.slice(0);
        for (j = 0; j < n-1; j += 1) {
          starts[j] += 1;
        }
        ends.push(size);
        starts.unshift(0);
        console.log(starts);
        console.log(ends);
        for (j = 0; j < n; j += 1) {
          map.push({start: starts[j], end: ends[j]});
        }
        j = 0;
        map.forEach(function(range) {
          finder('./O.Big.log', range.start, range.end, pattern, function(err, reduce) {
            var k;
            j = j + 1;
            if (err) {
              console.log(err.message);
            } else {
              for (k in reduce) {
                if (reduce.hasOwnProperty(k)) {
                  if (result.hasOwnProperty(k)) {
                    result[k] += reduce[k];
                  } else {
                    result[k] = reduce[k];
                  }
                }
              }
            }
            if (j === n) {
              console.log('it took ' + (Date.now()-now));
            }
          });
        });
      }
    });
  }
}


function test_finder(pattern) {
  'use strict';
  var fs = require('fs');
  var stream = require('stream').Stream;

  var size = fs.statSync('./O.Big.log').size;
  console.log('file size is ' + size);

  var now = Date.now();
  finder('./O.Big.log', 0, size, pattern, function(err,result) {
    var resultArray = [], k, i;
    if (err) {
      console.log(err);
    } else {
      // find top 10
      for (k in result) {
        resultArray.push({key: k, value: result[k]});
      }
      resultArray.sort(function(a, b) {
        return (b.value - a.value);
      });
      for (i = 0; i < 10; i += 1) {
        console.log(resultArray[i] )
      }
      console.log('it took ' + (Date.now()-now));
    }
  });
}


function test_findEnd(n) {
  'use strict';
  var fs = require('fs');
  var assert = require('assert');
  var size = fs.statSync('./O.Big.log').size;

  var position = Math.round(size / n);
  var now = Date.now();
  console.log('file size is ' + size + ', find the next line end from ' + position);
  findEnd('./O.Big.log', position, size, function(err, end) {
    if (err) {
      console.log(err.message);
    } else {
      console.log('start at ' + position + ' find ending at ' + end + ', time ' + (Date.now()-now));
      fs.open('./O.Big.log', 'r', function(err, fd) {
        if (err) {
          console.log(err.message);
        } else {
          fs.read(fd, new Buffer(end - position + 1), 0, (end - position + 1), position, function(err, bytesRead, buffer){
            assert.equal(buffer[end - position],'\n'.charCodeAt(0), 'findEnd does not work.');
          });
        }
      });
    }
  });
}

function finder(fileName, start, end, pattern, cb) {
  'use strict';
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
  'use strict';
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


