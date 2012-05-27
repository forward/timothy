require('../index')
    .map(function(key, value){
	updateStatus("\ngot key "+key+" -> "+value+"\n");
	emit(key, value);
    })
    .reduce(function(key,values){
	updateStatus("summing values for "+key);
	var acum = 0;
	for(var i=0; i<values.length; i++)
	    acum = acum + parseInt(values[i]);

	emit(key,acum);
    })
    //.run();
    .runLocal("/Users/antonio/Desktop/test2.txt");
