// Project name: Instajam
// Description: A platform to jam in real time with other musicians using MIDI devices
//
// I implemented a load balancer to manage load on subprocesses, and spawn new ones if necessary. The main advantage
// of doing this is to ensure that the socket emissions and receptions dont block each other and high amounts of
// traffic per second. There is a high percentage of time that our application exceeds the threshold for emissions
// per second due to the data-filled nature of playing music, therefore introducing latency to users. Currently, using
// callbacks in javascript will not take advantage of multi core environments and means that there will be synchronous
// execution on the hardware level of socket emissions, even if performed with staggered async execution.
//

var child_process = require('child_process');

// global variables instantiated to hold references to values that will be
// defined at socket emission but not at sub process routine instantiation.
var data;
var room;
var socket;

var numCPUs = require('os').cpus().length;

var LoadBalancer = function() {
  this.priorityQueue = [];
  this.processHash = {};

  for(var i = 1; i <= numCPUs; i++) {
    var procName = 'proc' + i;

    // spawning child processes and added the child objects to hash for later reference
    this.processHash[procName] = child_process.fork(__dirname + '/child.js');

    // inserting the names of all processes to our priority queue
    this.insert(procName);

    this.processHash[procName].on('message', function(m) {
      socket.broadcast.emit(room + ' event', data);
    });
  }
}

LoadBalancer.prototype.closeAllProcesses = function() {
  for(processName in this.processHash) {
    this.processHash[processName].kill();
  }
}

LoadBalancer.prototype.emit = function(data, room, socket) {
  // retrieve the lest busy process, use it to emit, and then
  // tell load balancer that it has performed its job
  var processToUse = this.addLoadToBestProcess();

  // object structure expected by the function loaded to each process
  var obj = JSON.stringify({
    'room': room,
    'data': data,
    'socket': socket
  });

  this.processHash[processToUse].send(obj);

  this.removeLoadFromProcess(processToUse);
}

// Time: O(log n)
LoadBalancer.prototype.insert = function(processName) {
  // perform dirty insert on the heap data structure
  var lastOpenIdx = this.priorityQueue.length;
  var processTuple = [processName, 0];
  this.priorityQueue[lastOpenIdx] = processTuple;

  // bubble the newly created process up the heap until it is at the top,
  // adjusting the position of processes already in the heap accordingly
  var recurseInsert = function(index, processTuple) {
    var parentIndex = this.parent(index);

    // base case to stop the insertion when we have reached the parent
    if(parentIndex < 0) {
      return;
    }

    var temp = this.priorityQueue[parentIndex];
    this.priorityQueue[parentIndex] = processTuple;
    this.priorityQueue[index] = temp;

    recurseInsert.call(this, parentIndex, processTuple);
  }

  // begin the recursive insertion to get the processes load correctly tracked
  recurseInsert.call(this, lastOpenIdx, processTuple);
}

// helper method for the remove operation
LoadBalancer.prototype.promoteSubtree = function(index) {
  var leftIndex = this.leftChild(index);
  var rightIndex = this.rightChild(index);
  var leftChild = this.priorityQueue[leftIndex];
  var rightChild = this.priorityQueue[rightIndex];

  if(leftChild && rightChild ) {
    if(leftChild[1] <= rightChild[1]) {
      this.priorityQueue[index] = leftChild;
      this.promoteSubtree.call(this, leftIndex);
    }
    else {
      this.priorityQueue[index] = rightChild;
      this.promoteSubtree.call(this, rightIndex);
    }
  }
  else if(leftChild) {
    this.priorityQueue[index] = leftChild;
    this.promoteSubtree.call(this, leftIndex);
  }
  else if(rightChild) {
    this.priorityQueue[index] = rightChild;
    this.promoteSubtree.call(this, rightIndex);
  }
  else {
    // the index has no children, delete it from the array
    delete this.priorityQueue[index];
  }
}

// removes a process name from the load balancer and adjusts the load balancer to
// correctly maintain data structure invariant for remaining processes
// Time: O(n)
LoadBalancer.prototype.remove = function(processName) {
  var recurseDFS = function(name, index) {
    var proc = this.priorityQueue[index];

    if(proc[0] === processName) {
      this.promoteSubtree.call(this, index);
    }
    else {
      var leftIndex = this.leftChild(index);
      var rightIndex = this.rightChild(index);

      if(this.priorityQueue[leftIndex]) {
        recurseDFS.call(this, name, leftIndex);
      }

      if(this.priorityQueue[rightIndex]) {
        recurseDFS.call(this, name, rightIndex);
      }
    }
  }

  recurseDFS.call(this, processName, 0)
}

// return the process name with the minimum amount of work
LoadBalancer.prototype.findMin = function() {
  return this.priorityQueue[0];
}

// finds the index of the left and right child of a parent at a given index
LoadBalancer.prototype.leftChild = function(i) {
  return 2*i + 1;
}

LoadBalancer.prototype.rightChild = function(i) {
  return 2*i + 2;
}
//

// finds the parent index of a child
LoadBalancer.prototype.parent = function(i) {
  return Math.floor((i-1)/2);
}

LoadBalancer.prototype.swapDirection = function(i, count) {
  if( this.priorityQueue[i] !== undefined ) {
    if( this.priorityQueue[this.leftChild(i)] && count > this.priorityQueue[this.leftChild(i)][1] ) {
      return 'left';
    } else if( this.priorityQueue[this.rightChild(i)] && count > this.priorityQueue[this.rightChild(i)][1] ) {
      return 'right';
    }
  }
}

// helper method
LoadBalancer.prototype.swapIndicesOnArray= function(idx1, idx2, array) {
  var temp = array[idx1];
  array[idx1] = array[idx2];
  array[idx2] = temp;
}

// when a user adds load to the process doing the least amount of work,
// make sure to increase the work count on the process and put process now doing
// the least amount of work to the top of the tree.
// Time: O(log n)
LoadBalancer.prototype.addLoadToBestProcess = function() {
  var processTuple = this.findMin();
  processTuple[1]++;

  // restrict sub processes to 8 process so that we will use up to 8 cores
  // if we have more
  var index = 0;
  var direction = this.swapDirection(index, processTuple[1]);

  while(direction) {
    var swapIdx;

    if(direction === 'left'){
      swapIdx = this.leftChild(index);
    }
    else {
      swapIdx = this.rightChild(index);
    }

    this.swapIndicesOnArray(index, swapIdx, this.priorityQueue);
    index = swapIdx;

    direction = this.swapDirection(index, processTuple[1]);
  }

  return processTuple[0];
}

// removes load from process name and correctly adjust load balancer
// Time: O(n), average O(log n)
LoadBalancer.prototype.removeLoadFromProcess = function(processName) {
  var recurseDFS = function(index) {
    if(index >= this.priorityQueue.length) { return; }

    var curProcess = this.priorityQueue[index];
    if(curProcess[0] === processName) {
      if(curProcess[1] > 0) {
        curProcess[1]--;
      }

      // if cur process drops below 0 load, free process up
      return true;
    }

    var leftSwap = this.leftChild(index);
    var rightSwap = this.rightChild(index);

    if(recurseDFS.call(this, leftSwap)) {
      if(this.priorityQueue[leftSwap][1] < curProcess[1]) {
        this.swapIndicesOnArray.call(this, index, leftSwap, this.priorityQueue);
        return true;
      }
    }
    else if(recurseDFS.call(this, rightSwap)) {
      if(this.priorityQueue[rightSwap][1] < curProcess[1]) {
        this.swapIndicesOnArray.call(this, index, rightSwap, this.priorityQueue);
        return true;
      }
    }

    return false;
  }

  recurseDFS.call(this, 0);
}

var mod = new LoadBalancer();

module.exports = mod;