#!/usr/bin/env node

//@REQUIRES_HERE

(function(){
    var emit = function(key, value){
	process.stdout.write(key + "\t" + value + "\n");  
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
	    for(var i=0; i<maxIter; i++)
		map(parts[i]);
	    acum = rest;
	}
    });

    process.stdin.on('end', function() {
	if (acum.length > 0)
	    map(acum);
    });

    process.stdin.setEncoding('utf8');
    process.stdin.resume();
})();