require('../index')
    .timothy
    .configure({
	
	config: './hadoop.xml',
	input: "/user/andy/hilton-top-keywords/attempt_201101071627_15643_r_000000_0",
	output: "/user/abhinay/wordcount/output2",
	name: "Timothy Word Count Example"
    })
    .map(function(key,value){
	var words = value.split(" ");
        for(var i=0; i<words.length; i++) {
	    emit(words[i], 1);
	}
    })
    .reduce(function(word,counts){
	emit(word, counts.length);
    })
    .run();
