/**************************/
/** Loading core modules **/
/**************************/

// module for file I/O operations
var fs = require('fs');
// module for handling and transforming file paths
var path = require('path');
// module for handling HTTP connections
var http = require('http');
// module for dealing with query strings
var querystring = require('querystring');
// module for reading console input
var readline = require('readline');

/*************************/
/** Declaring constants **/
/*************************/

// sparql endpoint host
var sparqlHost = 'ulcs.fusepool.info';
// sparql endpoint port
var sparqlPort = '8890';
// sparql endpoint path
var sparqlPath = '/sparql';
// sparql username for authentication
var sparqlUser = '';
// sparql password for authentication
var sparqlPass = '';
// basic auth header for sparql
var basicAuth = '';
// timeout parameter of sparql requests (15 mins)
var timeout = 15 * 60 * 1000;
// format parameter of sparql requests
var formats = {
    JSON: 'application/json',
    TURTLE: 'text/turtle',
    RDF: 'application/rdf+xml',
    FORM: 'application/x-www-form-urlencoded',
    AUTO: 'auto'
};
// parameter that needs to be replaced in the queries
var staticUri = 'http://d-nb.info/gnd/118529692';
// relative path of folder containing the sparql files
var resourceFolder = 'sparql';
// file names
var files = {
    T0: 'T0_select-all-GND-IDs-(milos).rq',
    T3: 'T3-new_data-from-DBpedia-(milos)-insert.rq'
};
// relative file paths
var filePaths = {
    T0: path.join(__dirname, resourceFolder, files.T0),
    T3: path.join(__dirname, resourceFolder, files.T3)
};

/********************/
/** Main variables **/
/********************/

// if false the script will read the URIs from the data
// folder using the resultFolder, otherwise runs T0 (from console input)
var RUN_T0;
// stored result of T0 query (data folder)
var resultFolder = './data/T0_all/';
// list of result files
var resultFiles = fs.readdirSync(resultFolder);
// current result file
var fileIndex = 0;
// global array for storing the list of URIs
var URIs = new Array();
// local index for looping through the list of URIs
var index = 0;
// global index for looping through the list of URIs
var globalIndex = 0;
// global variable to indicate first cycle after T0
var first = true;
// limit for T0 (only for query)
var limit = 1000;
// global offset for T0  (only for query)
var offset = 0;
// flags for synchronization
var readyFlags = {
    T0: false,
    T3: false
};
// sparql queries read from file
var queries = {
    T0: readQuery(filePaths.T0),
    T3: readQuery(filePaths.T3)
};

// STARTING SCRIPT
initialize();

/********************/
/** Main functions **/
/********************/

/**
 * Get user inputs before starting execution.
 */
function initialize() {
    console.log('\n---== Starting Script ==---');
    var rl = readline.createInterface({input: process.stdin, output: process.stdout});

    console.log('\nPlease provide credential for "' + sparqlHost + ':' + sparqlPort + sparqlPath + '" for basic HTTP authentication!');
    // get credentials from console input
    rl.question('Username: ', function (answer1) {
        sparqlUser = answer1;
        rl.question('Password: ', function (answer2) {
            sparqlPass = answer2;
            // run T0 from query or from file
            console.log('\nDo you want to execute T0 query? (If not the script will use the results in "' + resultFolder + '" folder!)');
            rl.question('Run T0? (y/n): ', function (answer3) {
                RUN_T0 = (answer3 == 'y' || answer3 == 'Y') ? true : false;
                rl.close();
                start();
            });
        });
    });
}

/**
 * Starts the script by getting the list of URIs either
 * querying it from the SPARQL endpoint using T0 or reading
 * it from file. (The file contains the result of T0 as plain text.) 
 **/
function start() {
    console.time('Execution time');
    basicAuth = 'Basic ' + new Buffer(sparqlUser + ':' + sparqlPass).toString('base64');
    if (RUN_T0) {
        console.log('\nExecuting T0 query...');
        queryURIs();
    }
    else {
        console.log('\nReading "' + resultFiles[fileIndex] + '" file...');
        readURIs();
    }
}

/**
 * Gets all the URIs using the T0 query.
 **/
function queryURIs() {
    var query = replaceAll(queries.T0, '<offset>', offset);
    query = replaceAll(query, '<limit>', limit);
    // empty array
    URIs = new Array();
    // get POST data
    var data = getData(query, formats.JSON);
    var options = getOptions(data)
    // create HTTP request
    var request = http.request(options, function (response) {
        response.setEncoding('utf8');
        response.on('data', function (data) {
            var json = JSON.parse(data);
            json = json.results.bindings;
            for (var i = 0; i < json.length; i++) {
                URIs.push(json[i].gndid.value);
            }
            // indicate first cycle after T0
            first = true;
            syncCallback();
        });
    });

    request.on('error', function (e) {
        console.error('FATAL: Request returned with error while querying T0.');
        process.exit(1);
    });

    request.write(data);
    request.end();
}

/**
 * Reads all the URIs from file. (Result of T0.)
 **/
function readURIs() {
    var filePath = path.join(resultFolder, resultFiles[fileIndex]);
    // empty array
    URIs = new Array();
    // read file asynchronously
    fs.readFile(filePath, 'utf-8', function (error, data) {
        // if file cannot be read abort with fatal error
        if (error) {
            console.error('FATAL: Could not read file "' + filePath + '"');
            process.exit(1);
        }
        // if data is empty abort with fatal error
        if (isEmpty(data)) {
            console.error('FATAL: File is empty "' + filePath + '"');
            process.exit(1);
        }
        // set global list of URIs
        URIs = data.split('\n');
		// clean array
		URIs = cleanArray(URIs);
        // indicate first cycle after T0
        first = true;
        syncCallback();
    });
}

/**
 * Replaces the static URI in the query and starts the first
 * it asynchronously.
 **/
function startAsyncQueries(currentUri) {
    console.log((globalIndex + 1) + '. <' + currentUri + '>');
    // T3 query
    query = replaceAll(queries.T3, staticUri, currentUri);
    query = replaceURISegments(query, staticUri, currentUri);
    runQuery(query, 'T3');
}

/**
 * Runs the supplied query asynchronously.
 **/
function runQuery(query, queryName) {
    var data = getData(query, formats.AUTO);
    var options = getOptions(data)
    // create HTTP request
    var request = http.request(options, function (response) {
        if (response.statusCode == 200) {
            console.log('\t' + queryName + ' done');
        }
        else {
            console.error('\tERROR: Response returned with status code ' + response.statusCode + ' for query "' + queryName + '"');
        }
        // set query ready flag
        readyFlags[queryName] = true;
        // synchronize with other threads
        syncCallback();
    });

    request.on('error', function (e) {
        console.error('\tERROR: Request returned with error for query "' + queryName + '"');
        readyFlags[queryName] = true;
        syncCallback();
    });

    request.write(data);
    request.end();
}

/**
 * Synchronizes the asynchronously started queries. It also keeps
 * the first and second group of queries from running concurrently.
 **/
function syncCallback() {
    if (readyFlags['T3']) {
        // reset ready flags
        readyFlags['T3'] = false;
        // increase index
        index++;
        // increase global index
        globalIndex++;
        // go to the next URI if there is any
        if (index < URIs.length) {
            // get current URI
            var currentUri = URIs[index];
            // start the first group of queries
            startAsyncQueries(currentUri);
            return;
        }
        else {
            if (RUN_T0) {
                if (URIs.length > 0) {
                    index = 0;
                    // increase offset for T0
                    offset += limit;
                    console.log('Executing T0 query...');
                    queryURIs();
                }
                else {
                    console.timeEnd('Execution time');
                    process.exit(0);
                }
            }
            else {
                if (fileIndex < resultFiles.length - 1) {
                    index = 0;
                    // increase file index
                    fileIndex++;
                    console.log('\nReading "' + resultFiles[fileIndex] + '" file...');
                    readURIs();
                }
                else {
                    console.timeEnd('Execution time');
                    process.exit(0);
                }
            }
        }
    }
    // if this is the first run (after T0)
    if (first) {
        first = false;
        console.log('Iterate through each GND-ID...');
        // get current URI
        var currentUri = URIs[index];
        // start the first group of queries
        startAsyncQueries(currentUri);
        return;
    }
}

/**
 * Replaces the last segment of uri1 with the last
 * segment of uri2 in the query text. (For query T32.)
 **/
function replaceURISegments(query, uri1, uri2) {
    // get last segment from uri1
    var str1 = uri1.substr(uri1.lastIndexOf("/") + 1);
    // get last segment from uri2
    var str2 = uri2.substr(uri2.lastIndexOf("/") + 1);
    // replace all
    return replaceAll(query, str1, str2);
}

/*********************/
/** Other functions **/
/*********************/

/**
 * Create URI encoded data for HTTP POST.
 **/
function getData(query, format) {
    return data = querystring.stringify({
        query: query,
        format: format,
        timeout: timeout
    });
}

/**
 * Create options for HTTP POST.
 **/
function getOptions(data) {
    return options = {
        host: sparqlHost,
        port: sparqlPort,
        path: sparqlPath,
        method: 'POST',
        headers: {
            'Authorization': basicAuth,
            'Content-Type': formats.FORM,
            'Content-Length': Buffer.byteLength(data)
        }
    };
}

/**
 * Reads file synchronously from supplied path.
 **/
function readQuery(filePath) {
    var data = fs.readFileSync(filePath, 'utf-8');
    if (isEmpty(data)) {
        console.error('FATAL: File is empty "' + filePath + '"');
        process.exit(1);
    }
    // remove comment lines
    data = data.replace(/^#.*$/gm, '');
    // remove blank lines	
    data = data.replace(/^\s*$/gm, '');
    return data;
}

/**
 * Replaces all occurrences of str1 to str2 in text.
 **/
function replaceAll(text, str1, str2) {
    var regex = new RegExp(str1, 'g');
    return text.replace(regex, str2);
}

/**
 * Checks if variable is empty.
 **/
function isEmpty(data) {
    if (typeof data === 'undefined' || data === '' || data === null || data.length == 0) {
        return true;
    }
    return false;
}

/**
 * Remove empty elements from array.
 **/
function cleanArray(actual) {
  var newArray = new Array();
  for (var i = 0; i < actual.length; i++) {
    if (actual[i]) {
      newArray.push(actual[i]);
    }
  }
  return newArray;
}