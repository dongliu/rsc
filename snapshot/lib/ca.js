var childproc = require('child_process'),
    EventEmitter = require('events').EventEmitter,
    fieldSeprator = ' ';

/*reuse the child process wrapper in node-imagemagick
https://github.com/rsms/node-imagemagick*/

function exec2(command, args /*, options, callback */ ) {
  var options = {
    encoding: 'utf8',
    timeout: 0,
    maxBuffer: 500 * 1024,
    killSignal: 'SIGKILL',
    output: null
  };

  var callback = arguments[arguments.length - 1];
  if ('function' != typeof callback) callback = null;

  if (typeof arguments[2] == 'object') {
    var keys = Object.keys(options);
    for (var i = 0; i < keys.length; i++) {
      var k = keys[i];
      if (arguments[2][k] !== undefined) {
        options[k] = arguments[2][k];
      }
    }
  }

  var child = childproc.spawn(command, args);
  var killed = false;
  var timedOut = false;

  var Wrapper = function(proc) {
      this.proc = proc;
      this.stderr = new Accumulator();
      proc.emitter = new EventEmitter();
      proc.on = proc.emitter.on.bind(proc.emitter);
      this.out = proc.emitter.emit.bind(proc.emitter, 'data');
      this.err = this.stderr.out.bind(this.stderr);
      this.errCurrent = this.stderr.current.bind(this.stderr);
      };
  Wrapper.prototype.finish = function(err) {
    this.proc.emitter.emit('end', err, this.errCurrent());
  };

  var Accumulator = function(cb) {
      this.stdout = {
        contents: ''
      };
      this.stderr = {
        contents: ''
      };
      this.callback = cb;

      var limitedWrite = function(stream) {
          return function(chunk) {
            stream.contents += chunk;
            if (!killed && stream.contents.length > options.maxBuffer) {
              child.kill(options.killSignal);
              killed = true;
            }
          };
          };
      this.out = limitedWrite(this.stdout);
      this.err = limitedWrite(this.stderr);
      };
  Accumulator.prototype.current = function() {
    return this.stdout.contents;
  };
  Accumulator.prototype.errCurrent = function() {
    return this.stderr.contents;
  };
  Accumulator.prototype.finish = function(err) {
    this.callback(err, this.stdout.contents, this.stderr.contents);
  };

  var std = callback ? new Accumulator(callback) : new Wrapper(child);

  var timeoutId;
  if (options.timeout > 0) {
    timeoutId = setTimeout(function() {
      if (!killed) {
        child.kill(options.killSignal);
        timedOut = true;
        killed = true;
        timeoutId = null;
      }
    }, options.timeout);
  }

  child.stdout.setEncoding(options.encoding);
  child.stderr.setEncoding(options.encoding);

  child.stdout.addListener('data', function(chunk) {
    std.out(chunk, options.encoding);
  });
  child.stderr.addListener('data', function(chunk) {
    std.err(chunk, options.encoding);
  });

  child.addListener('exit', function(code, signal) {
    if (timeoutId) clearTimeout(timeoutId);
    if (code === 0 && signal === null) {
      std.finish(null);
    } else {
      var e = new Error(std.errCurrent());
      e.timedOut = timedOut;
      e.killed = killed;
      e.code = code;
      e.signal = signal;
      std.finish(e);
    }
  });

  return child;
}

/*According to epics document, caget -a can output \"name timestamp value stat sevr\". However, some
 only output \"name timestamp value\". This parser will handle both cases. If colon, then the : in
 pv name will be replaced by - in order to be friend to CSS. */
/*TODO: handle arrays and waveform*/

exports.parseCagetA = function(input) {
  var pv = {},
      comps,
      size;
  if (input.length > 0) {
    comps = input.split(/\s+/);
    size = comps.length;
    if (size > 0 && size <= 6){
      if (size > 3) {
        pv.timestamp = new Date(comps[1] + ' ' + comps[2]);
        pv.value = comps[3];
        if (size == 6) {
          pv.stat = comps[4];
          pv.sevr = comps[5];
        } else {
          pv.stat = 'normal';
        }
      }
      pv.name = comps[0];
    }
  }
  return pv;
};

/*TODO: parse caget -d*/
exports.parseCagetD = function(input) {
  return null;
};


exports.parseCaget = function(input) {
  var pv = {},
      comps,
      size;
  if (input.length > 0) {
    comps = input.split(/\s+/);
    size = comps.length;
    if (size >= 2){
      pv.name = comps[0];
      pv.value = comps[1];
    }
  }
  return pv;
};

exports.parseCainfo = function(input) {
  return null;
};

exports.exec = function(command, pv, options, callback) {
  var args;
  if (arguments.length == 3) {
    args = [pv];
    callback = options;
  } else {
    args = ([options]).concat(pv);
  }
  var proc = exec2(exports.path[command], args, {
    timeout: 120000
  }, function(err, stdout, stderr) {
    callback(err, stdout);
  });
  return proc;
};
exports.path = {caget: 'caget',
                cainfo: 'cainfo'};

