#!/usr/bin/env node

//@REQUIRES_HERE

(function() {

    var emit = function() {
	var acum = [];
	for(var i=0; i<arguments.length; i++)
	    acum.push(arguments[i]);
	process.stdout.write(acum.join("\t")+"\n");  	    
    };

    var updateCounter = function(group, counter, amout) {
	process.stderr.write("reporter:counter:"+group+","+counter+","+amout);
    };

    var updateStatus = function(message) {
	process.stderr.write("reporter:status:"+message);
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
		if(line != null && line.length>0) {
		    comps = line.split("\t");
		    key = comps[0];
		    value = comps[1];

		    if(currentKey != key) {
			if(currentKey != null)
			    reduce(currentKey, currentBatch);
			currentKey = key;
			currentBatch = [value];
		    } else
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