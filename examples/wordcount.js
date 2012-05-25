require('../index')
    .configure({	
	config: './hadoop.xml',
	input: "/test.txt",
	output: "/processed_"+(new Date().getTime()),
	name: "Timothy Word Count Example"
    })
    .dependencies({"node-uuid":"1.3.3"})
    .setup(function() {
	global.x = 0;
	global.inc = function() {
	    global.x = global.x + 1;
	};
	global.uuid =require('node-uuid');
    })
    .map(function(line){
	var words = line.split(" ");

	for(var i=0; i<words.length; i++) {
	    inc(); emit(words[i], x);
	}
    })
    .reduce(function(word,counts){
	emit(word, counts.length);
        emit(uuid.v1(),"10000000");
    })
    //.run();
    .runLocal("/Users/antonio/Desktop/test.txt");
