var fs = require('fs');
var exec = require('child_process').exec;
var LocalDriver = function(jobDescription) {
    this.jobDescription = jobDescription;
};

LocalDriver.prototype.execute = function(i, input, output, cb) {
    var cmdenv = (this.jobDescription.configuration.cmdenv || '').split(',').join(' ');
    this.jobDescription.generate(i, function(err,job){
	if(err) {
	    cb(err, "Error generating map reduce scripts");
	} else {
	    var command;
	    var setup = "";
	    var inputPostfix = "";

	    try {
		if(fs.statSync(input).isDirectory())
		    inputPostfix = "/*";
	    } catch (x) {  }

	    if(i==0)
		setup = " && mkdir ./.npmcfg && npm config set userconfig ./.npmcfg && npm config set cache . &&  npm install > /dev/null";
	    if(output === null) {
		command = "cd "+job.jobWorkingDirectory+setup+" && cat "+input+inputPostfix+" | "+ cmdenv +" node "+job.mapperPath+" | sort | "+ cmdenv +" node "+job.reducerPath;
	    } else {
		command = "cd "+job.jobWorkingDirectory+setup+" && cat "+input+inputPostfix+" | "+ cmdenv +" node "+job.mapperPath+" | sort | "+ cmdenv +" node "+job.reducerPath +" > "+output;
	    }
	    console.log("** executing test command:\n"+command);
	    exec(command, function(e, stdout, stderr) {

		console.log("*** ERROR:\n");
		console.log(stderr);
		console.log("\n\n*** OUTPUT:\n");
		console.log(stdout);
				
		exec("rm -rf "+job.jobWorkingDirectory, function(error, stdout, stderr) {
		    if (error !== null) {
			console.log('(!!) exec error: ' + error);
			cb(error, "error removing tmp directory "+job.jobWorkingDirectory+" : "+error);
		    } else {
			cb(e, (e==null ?"Error executing test command:\n"+command : null));
		    }
		});
	    });
 	}
    });
};

exports.LocalDriver = LocalDriver;