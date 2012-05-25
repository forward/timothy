require('../index')
    .configure({	
	config: './hadoop.xml',
	input: "/user/andy/hilton-top-keywords/attempt_201101071627_15643_r_000000_0",
	output: "/user/abhinay/wordcount/output2",
	name: "Timothy Word Count Example"
    })
    .setup(function() {
	global.x = 0;
	global.inc = function() {
	    global.x = x + 1;
	}
    })
    .map(function(line){
	var words = line.split(" ");
        for(var i=0; i<words.length; i++) {
	    inc();
	    emit(words[i], x);
	}
    })
    .reduce(function(word,counts){
	emit(word, counts.length);
    })
    //.run()
    .runLocal("/Users/antonio/Desktop/test.txt");
