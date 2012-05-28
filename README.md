timothy: a Node.js library for building Hadoop jobs in JS
=========================================================

Timothy's primary goal is to make The Yellow Elephant rich and famous.


## Basic Example

```javascript
    // require timothy
    require('timothy')
        // basic configuration for the job: hadoop conf, input, output, name, etc
        .configure({	
             config: "./hadoop.xml",
             input:  "/test.txt",
             output: "/processed_"+(new Date().getTime()),
             name:   "Timothy Word Count Example"
        })
        // map function: one (line) or two (key, value) arguments
        .map(function(line){
            var words = line.split(" ");
            for(var i=0; i<words.length; i++)
                emit(words[i], 1); // emit is used to generate output
        })
        // reduce function: two arguments (key, value)
        .reduce(function(word,counts){
            emit(word, counts.length);
        })
        // run function, creates the job, uploads it and blocks until the
        // the execution has finished
        .run();
```

## Testing in the local machine

```javascript
    require('timothy')
        .map(function(line){
            var words = line.split(" ");
            for(var i=0; i<words.length; i++)
                emit(words[i], 1);
        })
        .reduce(function(word,counts){
            emit(word, counts.length);
        })
        // runLocal can be used instead of run to simulte the job execution 
        // from the command line
        .runLocal("~/Desktop/test_input.txt");
```

## Initialising a job

```javascript
    require('timothy')
        .configure({	
             config: "./hadoop.xml",
             input:  "/test.txt",
             output: "/processed_"+(new Date().getTime()),
             name:   "Timothy Word Count Example"
        })
        // variables and functions added to the global object will be available
        // in the map and reduce functions
        .setup(function(){
            global.x = 0;
            global.inc = function() {
                global.x = global.x + 1;
            };
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
        .run();
```

## Using node libraries

```javascript
    require('timothy')
        .configure({	
             config: "./hadoop.xml",
             input:  "/test.txt",
             output: "/processed_"+(new Date().getTime()),
             name:   "Timothy Word Count Example"
        })
        // Libraries can be added using the same syntax as
        // in a NPM package.json file
        .dependencies({"node-uuid":"1.3.3"})
        .setup(function(){
            // libraries can be required in the setup function
            global.uuid = require('node-uuid');
        })
        .map(function(line){
            var words = line.split(" ");
            for(var i=0; i<words.length; i++) {
                   emit(words[i], 1);
            }
        })
        .reduce(function(word,counts){
            emit(word, counts.length);
            emit(uuid.v1(),"10000000");
        })
        .run();
```

## Status and counters

Status and counters for the job can be updated using the *updateStatus* and *updateCounter* functions.