var events = require('events'); 
var util = require('util');

function Sim(){
  events.EventEmitter.call(this);  
}
util.inherits(Sim, events.EventEmitter);
exports.Sim = Sim;