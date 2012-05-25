#!/usr/bin/env node

//@REQUIRES_HERE

(function() {

    var emit = function(key,value) {
	process.stdout.write(key+"\t"+value+"\n");  
    };

    //@LOCALS_HERE

    //@REDUCER_HERE

    var acum = "";
    var currentKey, currentBatch =[];

    process.stdin.on('data', function(data) {
	acum = acum + data;
	if(acum.indexOf("\n") !== -1) {
	    var parts = acum.split("\n");
	    var maxIter = (acum[acum.length-1] === "\n" ? parts.length : parts.length-1);
	    var rest  = (acum[acum.length-1] === "\n" ? "" : parts[acum.length-1]);
	    var line, key, value, comps;
	    for(var i=0; i<maxIter; i++) {
		line = parts[i];
		comps = line.split("\t");
		key = comps[0];
		value = comps[1];

		if(currentKey != key) {
		    if(currentKey != null) {
			reduce(currentKey, currentBatch);
		    } 
		    currentKey = key;
		    currentBatch = [value];
		} else {
		    currentBatch.push(value);
		}
	    }
	    acum = rest;
	}
    });

    process.stdin.on('end', function() {
	reduce(currentKey, currentBatch);
    });

    process.stdin.setEncoding('utf8');
    process.stdin.resume();

})();