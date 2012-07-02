require('../index')
    .configure({	
	config: './hadoop.xml',
	input: "/tmp/loremipsum.txt",
	output: "/tmp/processed_"+(new Date().getTime()),
	name: "Timothy Word Count Example",
        cmdenv: "var=",
	"mapred.map.tasks": 10
    })
    .dependencies({"node-uuid":"1.3.3"})
    .setup(function() {
	x = 0;
	inc = function() {
	    x = x + 1;
	};
	uuid =require('node-uuid');
	this.updateStatus("setup...");
    })
    .map(function(line){
	var words = line.split(" ");

	for(var i=0; i<words.length; i++) {
	    this.updateStatus("mapping "+i);
	    inc(); 
	    this.emit(words[i], x);
	}
    })
    .reduce(function(word, counts){
	this.updateStatus("reducing "+word);
	this.emit(word, counts.length);
        this.emit(uuid.v1(),"10000000");
    })
    .run(function(err){
	     console.log("**FINISHED");
	     if (err !== null)
		 console.log(err);
    });
    //.runLocal("/Users/abhinay/work/timothy/examples/loremipsum.txt");

console.log("I should be seen straight away!");