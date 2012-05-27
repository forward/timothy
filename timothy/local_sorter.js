#!/usr/bin/env node

//@REQUIRES_HERE

var emit = function(key, value){
    process.stdout.write(key + "\t" + value + "\n");  
};

var data = {};
var keys = [];


var sort = function(line) {
    var parts = line.split("\t");
    var key = parts[0];
    var value = parts[1];

    var coll = data[key];
    if(coll == null) {
	keys.push(key);
	coll = [];
	data[key] = coll;
    }
    coll.push(value);
};


var acum = "";
process.stdin.on('data', function(data) {
		     acum = acum + data;
		     if(acum.indexOf("\n") !== -1) {
			 var parts = acum.split("\n");
			 var maxIter = (acum[acum.length-1] === "\n" ? parts.length : parts.length-1);
			 var rest  = (acum[acum.length-1] === "\n" ? "" : parts[acum.length-1]);
			 for(var i=0; i<maxIter; i++) {
			     if(parts[i] != null && parts[i].length > 0)
				 sort(parts[i]);
			 }
			 acum = rest;
		     }
		 });

process.stdin.on('end', function() {
    if (acum.length > 0)
	sort(acum);

    // sort and emit
    var coll;
    keys = keys.sort();
    for(var i=0; i<keys.length; i++) {
	coll = data[keys[i]];
	for(var j=0; j<coll.length; j++)
	    emit(keys[i],coll[j]);
    }

});


process.stdin.setEncoding('utf8');
process.stdin.resume();