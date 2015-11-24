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
// module for handling raw data
require('buffer');

/*************************/
/** Declaring constants **/
/*************************/

// sparql endpoint host
var sparqlHost = 'ulcs.fusepool.info';
// sparql endpoint port
var sparqlPort = '8890';
// sparql endpoint path
var sparqlPath = '/sparql';
// TLDPC host
var tldpcHost = 'ulcs.fusepool.info';
// TLDPC port
var tldpcPort = '8181';
// TLDPC path
var tldpcPath = '/DAV/home/fusepool/ldp/FU_Berlin_Extraction_Pipeline_Container/';
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
    T6: 'T6_construct-query.rq'
};
// relative file paths
var filePaths = {
    T0: path.join(__dirname, resourceFolder, files.T0),
    T6: path.join(__dirname, resourceFolder, files.T6)
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
// limit for T0 query
var limit = 1000;
// global offset for T0
var offset = 0;
// limit how many results is sent in one post
var postLimit = 100;
// current post index
var postIndex = 0;
// body of the post
var postBody = '';
// sparql queries read from file
var queries = {
    T0: readQuery(filePaths.T0),
    T6: readQuery(filePaths.T6)
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
    // run T0 from query or from file
    console.log('\nDo you want to execute T0 query? (If not the script will use the results in "' + resultFolder + '" folder!)');
    rl.question('Run T0? (y/n): ', function (answer3) {
        RUN_T0 = (answer3 == 'y' || answer3 == 'Y') ? true : false;
        rl.close();
        start();
    });
}

/**
 * Starts the script by getting the list of URIs either
 * querying it from the SPARQL endpoint using T0 or reading
 * it from file. (The file contains the result of T0 as plain text.) 
 **/
function start() {
    console.time('Execution time');
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
 * Runs the select query to retrieve results from the sparql endpoint.
 **/
function getGNDResult(query, currentUri) {
    var id = currentUri.substr(currentUri.lastIndexOf('/') + 1);
    console.log((globalIndex + 1) + '. <' + currentUri + '>');
    var data = getData(query, formats.TURTLE);
    var options = getOptions(data)
    // create HTTP request
    var request = http.request(options, function (response) {
        if (response.statusCode == 200) {
            var data = '';
            response.setEncoding('utf8');
            response.on('data', function (chunk) {
                data += chunk;
            });
            response.on('end', function () {
                console.log('\t' + id + ' select done');
				postBody += data + '\n';
				postIndex++;
				if(postIndex >= postLimit){
					postGNDResult();
				}
				else{
					syncCallback();
				}
            });
        }
        else {
            console.error('\tERROR: Response returned with status code ' + response.statusCode + ' for ' + id);
            syncCallback();
        }
    });

    request.on('error', function (e) {
        console.error('\tERROR: Request returned with error for ' + id);
        syncCallback();
    });

    request.write(data);
    request.end();
}

/**
 * Posts the selected results to the TLDPC.
 **/
function postGNDResult() {
    var options = {
        host: tldpcHost,
        port: tldpcPort,
        path: tldpcPath,
        method: 'POST',
        headers: {
            'Slug': (globalIndex - postIndex + 1) + '-' + globalIndex + '.ttl',
            'Content-Type': formats.TURTLE,
            'Content-Length': Buffer.byteLength(postBody)
        }
    };
    // create HTTP request
    var request = http.request(options, function (response) {
        if (response.statusCode == 201) {
            console.log('\nPosting results from ' + (globalIndex - postIndex + 1) + ' to ' + globalIndex + '\n');
        }
        else {
            console.error('\nERROR: Response returned with status code ' + response.statusCode + '\n');
        }
		
		postBody = '';
		postIndex = 0;
		syncCallback();
    });

    request.on('error', function (e) {
        console.error('\nERROR: Request returned with error for POST\n');
    });

    request.write(postBody);
    request.end();
}

/**
 * Synchronizes the asynchronously started queries. It also keeps
 * the first and second group of queries from running concurrently.
 **/
function syncCallback() {
    // go to the next URI if there is any
    if (index < URIs.length) {
        // get current URI
        var currentUri = URIs[index];
        var query = replaceAll(queries.T6, staticUri, currentUri);
        getGNDResult(query, currentUri);
        // increase index
        index++;
        // increase global index
        globalIndex++;
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
				if(postIndex > 0){
					postGNDResult();
				}
				else{
					console.timeEnd('Execution time');
					process.exit(0);
				}
            }
        }
    }
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