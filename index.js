var fs = require('fs');
var exec = require('child_process').exec;

var LocalDriver = function(jobDescription) {
    this.jobDescription = jobDescription;
};

LocalDriver.prototype.execute = function(cb) {
    this.jobDescription.generate(function(err,job){
	if(err) {
	    cb(err, "Error generating map reduce scripts");
	} else {
	    var command = "cat "+job.configuration.input+" | node "+job.mapperPath+" | node "+ __dirname+"/timothy/local_sorter.js | node "+job.reducerPath;
	    console.log("** executing test command:\n"+command);
	    exec(command, function(e, stdout, stderr) {

		fs.unlinkSync(job.mapperPath);
		fs.unlinkSync(job.reducerPath);

		console.log("\n\n*** OUTPUT:\n");
		console.log(stdout);
		console.log("*** ERROR:\n");
		console.log(stderr);
		cb(e==null, (e==null ?"Error executing test command:\n"+command : null));
	    });
	}
    });
};

var JobDescription = function() { };

/**
 * Validates description and throw meaningful errors
 */
JobDescription.prototype.validate = function(cb) {
    console.log("** validating");
    var notifications = {errors:[], warnings:[]};
    if(this.configuration.mapper == null)
	notifications.errors.push("ERROR: No mapper function has been assigned");

    if(this.configuration.hadoopHome == null)
	notifications.errors.push("ERROR: HADOOP_HOME not set");

    if(this.configuration.input == null)
	notifications.errors.push("ERROR: input not set");

    if(this.configuration.output == null)
	notifications.errors.push("ERROR: output not set");

    // add more errors and warnings

    if(notifications.errors.length > 0) {
	console.log("* validation ok");
	cb(false, notifications)
    } else {
	console.log("* validation fail");
	cb(true)
    }
};


/**
 * Generates map/reduce scripts and package them
 */
JobDescription.prototype.generate = function(cb) {
    console.log("* generating");
    this.mapperPath = __dirname+"/mapper_"+(new Date().getTime())+".js"
    this.reducerPath = __dirname+"/reducer_"+(new Date().getTime())+".js"
    var that = this;

    fs.readFile(__dirname+"/timothy/mapper_template.js", function (err, data) {
	data = data.toString().replace("//@MAPPER_HERE", "var map="+that.mapper+";")

	if(that.setupFn != null)
	    data = data.replace("//@LOCALS_HERE","var _that=this;("+that.setupFn.toString().replace(/global/g,"_that")+")();");

	fs.writeFile(that.mapperPath, data, function (err) {

	    fs.readFile(__dirname+"/timothy/reducer_template.js", function (err, data) {
		data = data.toString().replace("//@REDUCER_HERE", "var reduce="+that.reducer+";")

		if(that.setupFn != null)
		    data = data.replace("//@LOCALS_HERE","var _that=this; ("+that.setupFn.toString().replace(/global/g,"_that")+")();");

		fs.writeFile(that.reducerPath, data, function (err) {

		    cb(false, that);

		})
	    })

	});	
    });
};


JobDescription.prototype.execute = function(cb) {
    console.log("* executing");
    var command = this.configuration.hadoopHome+"/bin/hadoop jar "+this.configuration.hadoopHome+"/contrib/streaming/hadoop*streaming*.jar ";
    command += "-D mapred.job.name='"+this.configuration.name+"' ";
    command += "-files "+this.mapperPath+","+this.reducerPath+" ";
    if(this.configuration.config)
	command += "-conf '"+this.configuration.config+"' ";
    command += "-inputformat '"+this.configuration.inputFormat+"' ";
    command += "-outputformat '"+this.configuration.outputFormat+"' ";
    command += "-input "+this.configuration.input+" ";
    var mapperScript = this.mapperPath.split("/");
    mapperScript = mapperScript[mapperScript.length-1]
    command += "-mapper 'node "+mapperScript+"' ";
    var reducerScript = this.reducerPath.split("/");
    reducerScript = reducerScript[reducerScript.length-1]
    command += "-reducer 'node "+reducerScript+"' ";
    command += "-output "+this.configuration.output+" ";

    console.log("** executing Hadoop command:\n"+command);

    exec(command, function(e, stdout, stderr) {
	console.log("\n\n*** OUTPUT:\n");
	console.log(stdout);
	console.log("*** ERROR:\n");
	console.log(stderr);

	cb(e==null, (e==null ?"Error executing Hadoop command:\n"+command : null));
    });
};


var timothy = exports;

timothy.currentJob = null;

timothy.configure = function(options) {
    if(this.currentJob === null)
	this.currentJob = new JobDescription();

    this.currentJob.configuration = options;
    this.currentJob.configuration.inputFormat = "org.apache.hadoop.mapred.TextInputFormat"
    this.currentJob.configuration.outputFormat = "org.apache.hadoop.mapred.TextOutputFormat"
    if(this.currentJob.configuration.name == null)
	this.currentJob.configuration.name = "Timothy's MapReduce Job"
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
    this.currentJob.setupFn = setupFn;
    return this;
};

timothy.map = function(mapFn) {
    if(this.currentJob === null)
	this.currentJob = new JobDescription();
    
    this.currentJob.mapper = mapFn.toString();
    return this;
};

timothy.reduce = function(reduceFn) {
    if(this.currentJob === null)
	this.currentJob = new JobDescription();

    this.currentJob.reducer = reduceFn.toString();
    return this;
};

timothy.run = function(cb) {
    var that = this;
    cb = (cb == null ? function(){} : cb);

    this.currentJob.validate(function(err, msg){
	
	if(err) {
	    cb(err, msg);
	} else {
	    that.currentJob.generate(function(err, job){
		if(err) {
		    cb(err, job);
		} else {
		    job.execute(function(e,msg) {
			if(e) {
			    cb(true);
			} else {
			    fs.unlinkSync(that.currentJob.mapperPath);
			    fs.unlinkSync(that.currentJob.reducerPath);
			    // ready to process another job
			    that.currentJob = null;
			    // normal return
			    cb(false);
			}
		    });
		}
	    });
	}
    });
};

timothy.runLocal = function(inputPath, cb) {
    cb = (cb==null ? function(){} : cb);

    if(inputPath == null)
	throw new Exception("A local input path must be provided");
    else
	this.currentJob.configuration.input = inputPath;
	
    var driver = new LocalDriver(this.currentJob);
    driver.execute(cb);
};
