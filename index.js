var fs = require('fs');
var exec = require('child_process').exec;

Utils = {};
Utils.stackCounterLimit = 1000;
Utils.stackCounter = 0;

Utils.recur = function(c){
    if(Utils.stackCounter === Utils.stackCounterLimit) {
        Utils.stackCounter = 0;
        setTimeout(c, 0);
    } else {
        Utils.stackCounter++;
        c();
    }
};

Utils.repeat = function(c,max,floop,fend,env) {
    if(arguments.length===4) { env = {}; }
    if(c<max) {
        env._i = c;
        floop(function(floop,env){
            // avoid stack overflow
            // deadly hack
            Utils.recur(function(){ Utils.repeat(c+1, max, floop, fend, env); });
        },env);
    } else {
        fend(env);
    }
};

var LocalDriver = function(jobDescription) {
    this.jobDescription = jobDescription;
};

LocalDriver.prototype.execute = function(i, input, output, cb) {
    this.jobDescription.generate(i, function(err,job){
	if(err) {
	    cb(err, "Error generating map reduce scripts");
	} else {
	    var command;
	    var setup = "";
	    if(i==0)
		setup = " && mkdir ./.npmcfg && npm config set userconfig ./.npmcfg && npm config set cache . &&  npm install > /dev/null"
	    if(output === null) {
		command = "cd "+job.jobWorkingDirectory+setup+" && cat "+input+" | node "+job.mapperPath+" | node "+ __dirname+"/timothy/local_sorter.js | node "+job.reducerPath;
	    } else {
		command = "cd "+job.jobWorkingDirectory+setup+" && cat "+input+" | node "+job.mapperPath+" | node "+ __dirname+"/timothy/local_sorter.js | node "+job.reducerPath +" > "+output;
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

var JobDescription = function() { 
    this.filesToCompress = [];
    this.mappers = [];
    this.reducers = [];
};

/**
 * Validates description and throw meaningful errors
 */
JobDescription.prototype.validate = function(cb) {
    console.log("** validating");
    var notifications = {errors:[], warnings:[]};
    if(this.mappers.length === 0)
	notifications.errors.push("ERROR: No mapper function has been assigned");

    if(this.configuration.hadoopHome == null)
	notifications.errors.push("ERROR: HADOOP_HOME not set");

    if(this.configuration.input == null)
	notifications.errors.push("ERROR: input not set");

    if(this.configuration.output == null)
	notifications.errors.push("ERROR: output not set");

    // add more errors and warnings

    if(notifications.errors.length > 0) {
	console.log("* validation fail");
	console.log(notifications.errors);
	cb("Validation fail", notifications.errors);
    } else {
	console.log("* validation ok");
	cb(null, notifications);
    }
};

JobDescription.prototype.generatePackageDotJSON = function(cb) {
    var pkg = {name: this.configuration.name.replace(/[^a-zA-Z0-9]/g,"_").toLowerCase(),
	       version: "1.0.0",
	       dependencies: this.dependencies,
	       engine: "node >=0.6.0"};

    this.packageJSONPath = this.jobWorkingDirectory+"/package.json";
    this.filesToCompress.push("package.json");

    fs.writeFile(this.packageJSONPath, JSON.stringify(pkg), function (err) {
	if (err !== null) {
	    console.log('(!!) error writing package.json : ' + err);
	}
	cb(err);
    });
};

JobDescription.prototype.generateShellScripts = function(cb) {
    var that = this;
    var mapperCommand = "#!/usr/bin/env bash\n tar -zxvf compressed.tar.gz > /dev/null && node mapper.js";
    fs.writeFile(this.mapperShellScriptPath, mapperCommand, function (err) {
	if (err !== null) {
	    console.log('(!!) error writing package.json : ' + err);
	    cb(err);
	} else {
	    var reducerCommand = "#!/usr/bin/env bash\n tar -zxvf compressed.tar.gz > /dev/null && node reducer.js";
	    fs.writeFile(that.reducerShellScriptPath, reducerCommand, function (err) {
		if (err !== null) {
		    console.log('(!!) error writing package.json : ' + err);
		}
		cb(err);
	    });
	}
    });
};

JobDescription.prototype.installLocalModules = function(cb) {
    var that = this;
    var command = "cd "+that.jobWorkingDirectory+" && npm install";
    console.log("** Installing local modules");
    exec(command, function(err, stdout, stderr) {
	cb(err);
    });
};

JobDescription.prototype.compressFiles = function(cb) {
    var that = this;
    var command  = "cd "+this.jobWorkingDirectory+" && tar --exclude compressed.tar.gz -zcvf "+this.compressedPath +" "+this.filesToCompress.join(" ");
    console.log("** Compressing package");
    exec(command, function(err, stdout, sterr) {
	cb(err);
    });
};

/**
 * Generates map/reduce scripts and package them
 */
JobDescription.prototype.generate = function(index, cb) {
    this.jobWorkingDirectory = "/tmp/timothy/"+this.configuration.name.replace(/[^a-zA-Z0-9]/g,"_").toLowerCase()+"_"+(new Date().getTime())+"_"+(Math.floor(Math.random() * 100000));
    this.mapperPath = this.jobWorkingDirectory+"/mapper.js";
    this.reducerPath = this.jobWorkingDirectory+"/reducer.js";
    this.mapperShellScriptPath = this.jobWorkingDirectory+"/mapper.sh";
    this.reducerShellScriptPath = this.jobWorkingDirectory+"/reducer.sh";
    this.compressedPath = this.jobWorkingDirectory+"/compressed.tar.gz";

    this.filesToCompress = this.filesToCompress.concat(["mapper.js", "reducer.js", "mapper.sh", "reducer.sh"]);
    if (this.dependencies != null) {
	this.filesToCompress.push("node_modules");
    }
    console.log("* generated files in: "+this.jobWorkingDirectory);

    var that = this;

    exec("mkdir -p "+this.jobWorkingDirectory, 
	 function (error, stdout, stderr) {
	     if (error !== null) {
		 console.log('(!!) exec error: ' + error);
		 cb(error);
	     } else {
		 data = fs.readFileSync(__dirname+"/timothy/mapper_template.js", "utf8");
		 data = data.replace("//@MAPPER_HERE", "var map="+that.mappers[index]+";");

		 if(that.setupFn != null)
		     data = data.replace("//@LOCALS_HERE","var _that=this;("+that.setupFn.toString().replace(/global/g,"_that")+")();");

		 fs.writeFileSync(that.mapperPath, data, "utf8");
		 
		 data = fs.readFileSync(__dirname+"/timothy/reducer_template.js", "utf8");
		 data = data.replace("//@REDUCER_HERE", "var reduce="+that.reducers[index]+";");

		 if(that.setupFn != null)
		     data = data.replace("//@LOCALS_HERE","var _that=this; ("+that.setupFn.toString().replace(/global/g,"_that")+")();");

		 fs.writeFileSync(that.reducerPath, data, "utf8");
		 
		 that.generatePackageDotJSON(function(error){
		     if(error) {
			 cb(error, that);
		     } else {
			 that.installLocalModules(function(error) {
			     if(error) {
				 cb(error, that);
			     } else {
				 that.generateShellScripts(function(error){
				     if(error) {
					 cb(error, that);							 
				     } else {
					 that.compressFiles(function(error){
					     cb(error, that);
					 });
				     }
				 });
			     }
			 });
		     }
		 });
	     }
	 });
};

JobDescription.prototype.execute = function(index, cb) {
    var jobName = this.configuration.name;
    var input, output;

    if(this.mappers.length > 1)
	jobName += " Stage " + index;

    if(index > 0 && this.mappers.length > 1)
	input = this.configuration.output + "_stage_" + (index - 1)
    else 
	input = this.configuration.input

    if(index < (this.mappers.length -1) )
	output = this.configuration.output + "_stage_" + index
    else
	output = this.configuration.output

    console.log("* executing");
    var command = this.configuration.hadoopHome+"/bin/hadoop jar "+this.configuration.hadoopHome+"/contrib/streaming/hadoop*streaming*.jar ";
    command += "-D mapred.job.name='"+jobName+"' ";

    if(this.configuration.numMapTasks != null)
	    command += "-D mapred.map.tasks="+this.configuration.numMapTasks+" ";

    if(this.configuration.numRedTasks != null)
	    command += "-D mapred.reduce.tasks="+this.configuration.numMapTasks+" ";

    for(var prop in this.configuration) {
	if(prop.indexOf(".") != null && prop.split(".").length > 2) {
	    command += "-D "+prop+"="+this.configuration[prop]+" ";
	}
    }

    command += "-files "+this.compressedPath+","+this.mapperShellScriptPath+","+this.reducerShellScriptPath+" ";
    if(this.configuration.config)
	command += "-conf '"+this.configuration.config+"' ";
    command += "-inputformat '"+this.configuration.inputFormat+"' ";
    command += "-outputformat '"+this.configuration.outputFormat+"' ";
    if(input.constructor === Array) {
	for(var i=0; i<input.length; i++)
	    command += "-input "+input[i]+" ";	    
    } else {
	command += "-input "+input+" ";
    }

    if(this.configuration.combiner != null)
	command += "-combiner "+this.configuration.combiner+" ";
    if(this.configuration.dfs != null)
	command += "-dfs "+this.configuration.dfs+" ";
    if(this.configuration.jt != null)
	command += "-jt "+this.configuration.jt+" ";
    if(this.configuration.partitioner != null)
	command += "-partitioner "+this.configuration.partitioner+" ";
    if(this.configuration.cmdenv != null)
	command += "-cmdenv "+this.configuration.cmdenv+" ";
    
    var mapperScript = this.mapperPath.split("/");
    mapperScript = mapperScript[mapperScript.length-1]
    command += "-mapper 'mapper.sh' ";
    var reducerScript = this.reducerPath.split("/");
    reducerScript = reducerScript[reducerScript.length-1]
    command += "-reducer 'reducer.sh' ";
    command += "-output "+output+" ";

    console.log("** executing Hadoop command:\n"+command);

    exec(command, function(e, stdout, stderr) {
	console.log("\n\n*** OUTPUT:\n");
	console.log(stdout);
	console.log("*** ERROR:\n");
	console.log(stderr);

	cb(e, (e!==null ? "Error executing Hadoop command:\n"+command : null));
    });
};


var timothy = exports;

timothy.currentJob = null;

timothy.configure = function(options) {
    if(this.currentJob === null)
	this.currentJob = new JobDescription();

    this.currentJob.configuration = options;
    this.currentJob.configuration.inputFormat = "org.apache.hadoop.mapred.TextInputFormat";
    this.currentJob.configuration.outputFormat = "org.apache.hadoop.mapred.TextOutputFormat";
    if(this.currentJob.configuration.name == null)
	this.currentJob.configuration.name = "Timothy's MapReduce Job";
    if(this.currentJob.configuration.hadoopHome == null)
	this.currentJob.configuration.hadoopHome = process.env.HADOOP_HOME;
    return this;
};

timothy.define = function(defFn) {
    if(this.currentJob === null)
	this.currentJob = new JobDescription();

    var vars={}, fns={};

    defFn(vars,fns);

    this.currentJob.definitions = {vars: vars, fns: fns};

    return this;
};

timothy.setup = function(setupFn) {
    if(this.currentJob === null)
	this.currentJob = new JobDescription();

    this.currentJob.setupFn = setupFn;
    return this;
};

timothy.dependencies = function(dependenciesHash) {
    if(this.currentJob === null)
	this.currentJob = new JobDescription();

    this.currentJob.dependencies = dependenciesHash;
    return this;
};

timothy.map = function(mapFn) {
    if(this.currentJob === null)
	this.currentJob = new JobDescription();
    
    this.currentJob.mappers.push(mapFn.toString());
    return this;
};

timothy.reduce = function(reduceFn) {
    if(this.currentJob === null)
	this.currentJob = new JobDescription();

    this.currentJob.reducers.push(reduceFn.toString());
    return this;
};

timothy.run = function(cb) {
    var that = this;
    var errors = []

    cb = (cb == null ? function(){} : cb);

    this.currentJob.validate(function(err, msg){
	if(err) {
	    cb(err, msg);
	} else {
	    Utils.repeat(0,that.currentJob.mappers.length,function(k,env) {
		var floop = arguments.callee;
		if(errors.length == 0) {
		    var i = env._i;

		    that.currentJob.generate(i, function(err, job){
			if(err) {
			    errors.push(err);
			    k(floop,env);
			} else {
			    job.execute(i, function(e,msg) {
				if(e) {
				    errors.push(e);
				    k(floop,env);
				} else {
				    exec("rm -rf "+that.currentJob.jobWorkingDirectory, function(error, stdout, stderr) {
					if (error !== null) {
					    console.log('exec error: ' + error);
					    errors.push("error removing tmp directory "+that.currentJob.jobWorkingDirectory+" : "+error);
					}
					// success
					k(floop,env);
				    });
				}
			    });
			}
		    });
		} else {
		    k(floop,env);
		}
	    },
	    function(env) {
		that.currentJob = null;
		if (errors.length === 0)
		    cb(null);
		else
		    cb(errors)
	    })
	    //for(var i=0; i<that.currentJob.mappers.length; i++) {
	    //}
	    // ready to process another job
	}
    });
};

timothy.runLocal = function(inputPath, cb) {
    cb = (cb==null ? function(){} : cb);

    if(inputPath == null)
	throw new Exception("A local input path must be provided");
    else {
	if(this.currentJob.configuration == null)
	    this.currentJob.configuration = {};
	if(this.currentJob.configuration.name == null)
	    this.currentJob.configuration.name = "Timothy local job";
	this.currentJob.configuration.input = inputPath;
    }
	
    var driver = new LocalDriver(this.currentJob);
    var tmpDirectory = "/tmp/"+(new Date().getTime())+"_"+(Math.floor(Math.random() * 100000))+"local_timothy_run";
    var foundError = null;
    var that = this;

    exec("mkdir "+tmpDirectory, function(e, stdout, stderr) {
	if(e!=null) {
	    cb(e,"Unable to create temporary directory");
	} else {

	    Utils.repeat(0,that.currentJob.mappers.length, function(k,env) {
		if(foundError === null) {
		    var index = env._i;
		    var floop = arguments.callee;
		    var input,output;
		    
		    if(index > 0 && that.currentJob.mappers.length > 1)
			input = tmpDirectory + "/stage_" + (index - 1)
		    else 
			input = that.currentJob.configuration.input

		    if(index < (that.currentJob.mappers.length -1) )
			output = tmpDirectory + "/stage_" + index
		    else
			output = null

		    driver.execute(index,input,output,function(err){
			foundError = err;
			k(floop, env);
		    });
		} else {
		    k(floop,env);
		}
	    }, function(env) {
		cb(foundError, (foundError === null ? "" : "There was an error executing the jobs"));
	    });

	}
    });
};
