require('../index')
    .configure({	
	config: './hadoop.xml',
	input: "/tmp/loremipsum.txt",
	output: "/tmp/processed_"+(new Date().getTime()),
	name: "Timothy Word Count Example",
        cmdenv: "var=",
	"mapred.map.tasks": 10
    })
    .setup(function() {
	global.x = 0;
	global.inc = function() {
	    global.x = global.x + 1;
	};
	updateStatus("setup...");
    })
    .map(function(line){
	var words = line.split(" ");

	for(var i=0; i<words.length; i++) {
	    updateStatus("mapping "+i);
	    inc(); emit(words[i], x);
	}
    })
    .reduce(function(word,counts){
	updateStatus("reducing "+word);
	emit(word, counts.length);
    })
    .map(function(word, count){
	if (parseInt(count) > 1)
	    emit(word, count);
    })
    .reduce(function(word, counts) {
	emit(word, counts[0]);
    })
    //.run(function(err){
    // 	     console.log("**FINISHED");
    // 	     if (err !== null)
    // 		 console.log(err);
    //});
    .runLocal("/Users/abhinay/work/timothy/examples/loremipsum.txt", function(e,msg){
	if(e)
	    console.log("ERROR: "+e+" -> "+msg);
	else
	    console.log("OK");
    });
