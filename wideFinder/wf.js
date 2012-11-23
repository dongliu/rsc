
test_findEnd();

function test_findEnd() {
  var fs = require('fs'),
  assert = require('assert'),
  stream = require('stream').Stream;

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

function finder(fileName, start, end, pattern, cb) {
  var fs = require('fs'),
    stream = fs.createReadStream(fileName, {
      start: start,
      end: end
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

function findEnd(fileName, position, cb) {
  var fs = require('fs');
  var stream;
  stream = fs.createReadStream(fileName, {
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


