require('../index')
    .configure({	
	config: './hadoop.xml',
	input: "/user/andy/hilton-top-keywords/attempt_201101071627_15643_r_000000_0",
	output: "/user/abhinay/wordcount/output_"+(new Date().getTime()),
	name: "Timothy Word Count Example"
    })
    .dependencies({"node-uuid":"1.3.3"})
    .setup(function() {
	       global.x = 0;
	       global.inc = function() {
		   global.x = x + 1;
	       };
	       //global.uuid =require('node-uuid');
    })
    .map(function(line){
	     var exec = require('child_process').exec
	     exec("ls -l", function(error, stdout, stderr){
		      process.stderr.write("STDOUT");
		      process.stderr.write(stdout);
		      process.stderr.write("STDERR");
		      process.stderr.write(stderr);
		      
		      exec('readlink ./mapper.js', function(error, stdout, stderr) {
							
			       process.stderr.write("GOT DESTINATION"+stdout );
			       var dst = stdout.split("mapper.js")[0];
			       process.stderr.write("DST: "+dst );														
			       exec('ls -l '+dst, function(error, stdout, stderr) {
					process.stderr.write("LISTING DESTINATION\n"+stdout );
					
					var words = line.split(" ");
					for(var i=0; i<words.length; i++) {
					    inc();
					    emit(words[i], 1);
					}
				    });
			       
			   });
		  });
    })
    .reduce(function(word,counts){
	emit("KEY:"+word, "THE COUNT:"+counts.length);
        emit(uuid.v1(),"10000000");
    })
    .run();
    //.runLocal("/Users/abhinay/Desktop/union.patch");
