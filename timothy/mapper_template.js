#!/usr/bin/env node

//@REQUIRES_HERE

(function(){
    var emit = function(key, value){
	process.stdout.write(key + "\t" + value + "\n");  
    };

    var updateCounter = function(group, counter, amout) {
	process.stderr.write("reporter:counter:"+group+","+counter+","+amout);
    };

    var updateStatus = function(message) {
	process.stderr.write("reporter:status:"+message);
    };


    var mapKeyValue = function(line) {
	if(line.length === 0)
	    return;
	if(map.length === 1) {
	    map(line);
	} else if(map.length === 2) {
	    var tokens = line.split("\t");
	    var key = tokens[0];
	    var value = tokens.slice(1, tokens.length).join("\t");
	    map(key, value);    
	} else {
	    throw new Error("Mapper function arity must be 1 or 2");
	}
    };

    //@LOCALS_HERE

    //@MAPPER_HERE

    var acum = "";
    process.stdin.on('data', function(data) {
	acum = acum + data;
	if(acum.indexOf("\n") !== -1) {
	    var parts = acum.split("\n");
	    var maxIter = (acum[acum.length-1] === "\n" ? parts.length : parts.length-1);
	    var rest  = (acum[acum.length-1] === "\n" ? "" : parts[acum.length-1]);
	    for(var i=0; i<maxIter; i++) {
		mapKeyValue(parts[i]);
	    }
	    acum = rest;
	}
    });

    process.stdin.on('end', function() {
	if (acum.length > 0)
	    mapKeyValue(acum);
    });

    process.stdin.setEncoding('utf8');
    process.stdin.resume();
})();