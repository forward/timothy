#!/usr/bin/env node

//@REQUIRES_HERE

(function(){

     this.emit = function(key, value){
	process.stdout.write(key + "\t" + value + "\n");  
     };

     this.updateCounter = function(group, counter, amout) {
	 process.stderr.write("reporter:counter:"+group+","+counter+","+amout);
     };

     this.updateStatus = function(message) {
	 process.stderr.write("reporter:status:"+message);
     };

    var ___mapKeyValue = function(line) {
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

    var ___acum = "";
    process.stdin.on('data', function(data) {
	___acum = ___acum + data;
	if(___acum.indexOf("\n") !== -1) {
	    var parts = ___acum.split("\n");
	    var maxIter = (___acum[___acum.length-1] === "\n" ? parts.length : parts.length-1);
	    var rest  = (___acum[___acum.length-1] === "\n" ? "" : parts[parts.length-1]);
	    for(var i=0; i<maxIter; i++) {
		___mapKeyValue(parts[i]);
	    }
	    ___acum = rest;
	}
    });

    process.stdin.on('end', function() {
	if (___acum.length > 0)
	    ___mapKeyValue(___acum);
    });

    process.stdin.setEncoding('utf8');
    process.stdin.resume();
})();