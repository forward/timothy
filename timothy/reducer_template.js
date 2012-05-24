#!/usr/bin/env node

//@REQUIRES_HERE

var emit = function(value) {
    process.stdout.write(value+"\n");  
};

//@REDUCER_HERE

var acum = "";
var currentKey, currentBatch =[];

process.stdin.on('data', function(data) {
		     acum = acum + data;
		     if(acum.indexOf("\n") != -1) {
			 acum = processInput(acum);
		     }
		 });

process.stdin.on('end', function() {
		     if (acum.length > 0)
			 processInput(acum);
		     reduce(currentKey, currentBatch);
		 });

process.stdin.setEncoding('utf8');
process.stdin.resume();
