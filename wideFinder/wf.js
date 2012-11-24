
test_findEnd();

function test_finder() {
  var fs = require('fs');
  var assert = require('assert');
  var stream = require('stream').Stream;

  var size = fs.statSync('./10k').size;

  var position = Math.round(size / 1000);
  // var now = Date.now();
  console.log('file size is ' + size + ', find the next line end from ' + position);
  findEnd('./10k', position, function(err, end) {
    if (err) {
      console.log(err.message);
    } else {
      console.log('the end position is ' + end);
      console.log(finder('./10k', 0, end, \\));
    }
  });
}


function test_findEnd() {
  var fs = require('fs');
  var assert = require('assert');
  var stream = require('stream').Stream;

  var size = fs.statSync('./10k').size;

  var position = Math.round(size / 2);
  var now = Date.now();
  console.log('file size is ' + size + ', find the next line end from ' + position);
  findEnd('./10k', position, function(err, end) {
    if (err) {
      console.log(err.message);
    } else {
      console.log('the position is ' + end + ', time ' + (Date.now()-now));
      fs.open('./10k', 'r', function(err, fd) {
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

function finder(fileName, start, end, pattern) {
  var fs = require('fs');
  var StringDecoder = require('string_decoder').StringDecoder;
  var decoder = new StringDecoder('utf8');
  var result = {};
  var stream = fs.createReadStream(fileName, {
      start: start,
      end: end
    });
  var newline = '\n'.charCodeAt(0);
  var left = new Buffer(0);
  stream.on('data', function(data) {
    var i, last = 0, line, match;
    for (i = 0; i < data.length; i += 1) {
      if(newline === data[i]) { 
        if (last === 0 && Buffer.isBuffer(left) && left.length !== 0) {
          line = Buffer.concat([left, data.slice(0, i)]).toString('utf8');
          left = new Buffer(0);
        } else {
          line = data.toString('utf8', last, i);
        }
        match = pattern.exec(line);
        if (match) {
          if (result[match[0]]) {
            result[match[0]] += 1;
          } else {
            result[match[0]] = 1;
          } 
        }
        last = i + 1;  
      }
    }
    left = Buffer.concat([left, data.slice(last)]); 
  });
  stream.on('end', function(){
    return result;
  });
  stream.on('error', function(err){
    throw err;
  });

}

function findEnd(fileName, position, cb) {
  var fs = require('fs');
  var stream = fs.createReadStream(fileName, {
    start: position
  });
  stream.on('data', function(data) {
    var newline = '\n'.charCodeAt(0);
    var i, found = false, last = position;
    for (i = 0; i < data.length; i += 1) {
      if(newline === data[i]) {
        // find an ending
        found = true;
        break;
      }
    }
    if (found) {
      stream.destroy();
      return cb(null, last + i);
    } else {
      last += i;
    }
  });
  stream.on('end', function(){
    return cb(new Error('not found any ending in the file'), null);
  });
  stream.on('error', function(err){
    return cb(err, null);
  });
}


