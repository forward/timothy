#!/usr/bin/env node

//@REQUIRES_HERE

(function() {
     this.emit = function(){
	 var acum = [];
	 for(var i=0; i<arguments.length; i++)
	     acum.push(arguments[i]);
	 process.stdout.write(acum.join("\t")+"\n");  	    
     };

     this.updateCounter = function(group, counter, amout) {
	 process.stderr.write("reporter:counter:"+group+","+counter+","+amout);
     };

     this.updateStatus = function(message) {
	 process.stderr.write("reporter:status:"+message);
     };

    //@LOCALS_HERE

    //@REDUCER_HERE

    var ___acum = "";
    var ___currentKey, ___currentBatch =[];

    process.stdin.on('data', function(data) {
	___acum = ___acum + data;
	if(___acum.indexOf("\n") !== -1) {
	    var parts = ___acum.split("\n");
	    var maxIter = (___acum[___acum.length-1] === "\n" ? parts.length : parts.length-1);
	    var rest  = (___acum[___acum.length-1] === "\n" ? "" : parts[parts.length-1]);
	    var line, key, value, comps;
	    for(var i=0; i<maxIter; i++) {
		line = parts[i];
		if(line != null && line.length>0) {
		    comps = line.split("\t");
		    key = comps[0];
		    value = comps[1];

		    if(___currentKey != key) {
			if(___currentKey != null)
			    reduce( ___currentKey, ___currentBatch);
			___currentKey = key;
			___currentBatch = [value];
		    } else
			___currentBatch.push(value);
		}
	    }
	    ___acum = rest;
	}
    });

    process.stdin.on('end', function() {
	reduce(___currentKey, ___currentBatch);
    });

    process.stdin.setEncoding('utf8');
    process.stdin.resume();

})();