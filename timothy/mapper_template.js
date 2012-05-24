#!/usr/bin/env node

//@REQUIRES_HERE

var emit = function(key, value){
    process.stdout.write(key + "\t" + value + "\n");  
};

//@MAPPER_HERE

var acum = "";
process.stdin.on('data', function(data) {
		     acum = acum + data;
		     if(acum.indexOf("\n") !== -1) {
			 acum = map(acum);
		     }
		 });

process.stdin.on('end', function() {
		     if (acum.length > 0)
			 map(acum);
		 });

process.stdin.setEncoding('utf8');
process.stdin.resume();