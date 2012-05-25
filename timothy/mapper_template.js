#!/usr/bin/env node

//@REQUIRES_HERE

(function(){
    var emit = function(key, value){
	process.stdout.write(key + "\t" + value + "\n");  
    };

     var mapKeyValue = function(line) {
	 if(map.length === 1) {
	     map(line);
	 } else if(map.length === 2) {
	     var tokens = line.split("\t");
	     var key = tokens[0];
	     var value = tokens.slice(1, tokens.length-1).join("\t");
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