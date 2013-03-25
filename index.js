var exec = require('child_process').exec;
var Utils = require(__dirname+'/utils').Utils;
var LocalDriver = require(__dirname+'/local_driver').LocalDriver;
var JobDescription = require(__dirname+'/job_description').JobDescription;

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
    var errors = [];

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
		    cb(errors);
	    });
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
			input = tmpDirectory + "/stage_" + (index - 1);
		    else 
			input = that.currentJob.configuration.input;

		    if(index < (that.currentJob.mappers.length -1) )
			output = tmpDirectory + "/stage_" + index;
		    else
			output = that.currentJob.configuration.output;

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
